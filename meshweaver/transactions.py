"""
MeshWeaver Distributed Two-Phase Commit (2PC) ACID Transaction Coordinator.
Coordinates multi-key and multi-partition distributed atomic transactions with
Optimistic Concurrency Control (OCC), version fencing tokens, and automatic rollback on conflict.
"""

import asyncio
import logging
import time
import uuid
from typing import Any, Dict, List, Optional, Set, Tuple

from meshweaver.models import (
    LogEntry,
    RaftCommandType,
    TxIsolationLevel,
    TxOperation,
    TxOperationType,
    TxPrepareResult,
    TxRecord,
    TxStatus,
    WALRecordType,
)

logger = logging.getLogger("meshweaver.transactions")


class TransactionContext:
    """
    Client-facing transactional session context.
    Buffers mutations (set, increment, delete) and snapshot reads locally
    until explicit commit or rollback.
    """

    def __init__(
        self,
        tx_id: str,
        coordinator: "TransactionCoordinator",
        isolation_level: TxIsolationLevel = TxIsolationLevel.SERIALIZABLE,
        timeout_seconds: float = 10.0,
    ):
        self.tx_id = tx_id
        self.coordinator = coordinator
        self.isolation_level = isolation_level
        self.timeout_seconds = timeout_seconds
        self.operations: List[TxOperation] = []
        self.read_set: Dict[str, Any] = {}
        self.write_set: Dict[str, Any] = {}
        self._closed: bool = False

    def set(self, key: str, value: Any, expected_version: Optional[int] = None) -> None:
        """Buffer a SET operation in transaction write set."""
        if self._closed:
            raise RuntimeError("Transaction is closed")
        self.operations.append(
            TxOperation(
                op_type=TxOperationType.SET,
                key=key,
                value=value,
                expected_version=expected_version,
            )
        )
        self.write_set[key] = value

    def increment(self, key: str, delta: int = 1, expected_version: Optional[int] = None) -> None:
        """Buffer an INCREMENT operation in transaction write set."""
        if self._closed:
            raise RuntimeError("Transaction is closed")
        self.operations.append(
            TxOperation(
                op_type=TxOperationType.INCREMENT,
                key=key,
                delta=delta,
                expected_version=expected_version,
            )
        )
        # Update buffered local write set
        curr = self.write_set.get(key, self.read_set.get(key, 0))
        try:
            self.write_set[key] = int(curr) + delta
        except (ValueError, TypeError):
            self.write_set[key] = delta

    def delete(self, key: str, expected_version: Optional[int] = None) -> None:
        """Buffer a DELETE operation in transaction write set."""
        if self._closed:
            raise RuntimeError("Transaction is closed")
        self.operations.append(
            TxOperation(
                op_type=TxOperationType.DELETE,
                key=key,
                expected_version=expected_version,
            )
        )
        self.write_set[key] = None

    async def get(self, key: str) -> Any:
        """
        Read value for key. Returns locally buffered write if present,
        otherwise fetches from coordinator state machine and registers in read set.
        """
        if self._closed:
            raise RuntimeError("Transaction is closed")
        if key in self.write_set:
            return self.write_set[key]
        val = await self.coordinator.tx_read_key(self.tx_id, key)
        self.read_set[key] = val
        return val

    async def commit(self) -> bool:
        """Execute 2PC prepare and commit phases."""
        if self._closed:
            raise RuntimeError("Transaction already closed")
        self._closed = True
        return await self.coordinator.commit_transaction(self.tx_id, self)

    async def rollback(self, reason: Optional[str] = None) -> bool:
        """Abort and rollback transaction."""
        if self._closed:
            return False
        self._closed = True
        return await self.coordinator.rollback_transaction(self.tx_id, reason=reason)

    async def __aenter__(self) -> "TransactionContext":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        if not self._closed:
            if exc_type is not None:
                await self.rollback(reason=str(exc_val))
            else:
                await self.commit()


class TransactionCoordinator:
    """
    Distributed 2-Phase Commit (2PC) Transaction Coordinator.
    Enforces multi-key mutual exclusion, OCC version validation, atomic commits,
    and automatic distributed rollback on abort or timeout.
    """

    def __init__(
        self,
        node_id: str,
        raft_engine: Optional[Any] = None,
        wal_engine: Optional[Any] = None,
        state_machine: Optional[Any] = None,
    ):
        self.node_id = node_id
        self.raft_engine = raft_engine
        self.wal_engine = wal_engine
        self.state_machine = state_machine
        self._active_txs: Dict[str, TxRecord] = {}
        self._key_locks: Dict[str, str] = {}         # key -> tx_id
        self._key_versions: Dict[str, int] = {}      # key -> version counter
        self._fencing_tokens: Dict[str, int] = {}    # key -> fencing token
        self._token_counter: int = 0
        self._lock = asyncio.Lock()
        self.total_started: int = 0
        self.total_committed: int = 0
        self.total_aborted: int = 0

    async def begin_transaction(
        self,
        isolation_level: TxIsolationLevel = TxIsolationLevel.SERIALIZABLE,
        timeout_seconds: float = 10.0,
    ) -> TransactionContext:
        """Initialize a new distributed transaction context."""
        tx_id = f"tx_{uuid.uuid4().hex[:12]}"
        tx_record = TxRecord(
            tx_id=tx_id,
            coordinator_id=self.node_id,
            status=TxStatus.ACTIVE,
            isolation_level=isolation_level,
            timeout_seconds=timeout_seconds,
            created_at=time.time(),
        )

        async with self._lock:
            self._active_txs[tx_id] = tx_record
            self.total_started += 1

        if self.wal_engine:
            await self.wal_engine.append(
                WALRecordType.TX_MARKER,
                {"action": "BEGIN", "tx_id": tx_id, "isolation_level": isolation_level.value},
            )

        logger.info(f"Transaction '{tx_id}' started with isolation={isolation_level.value}")
        return TransactionContext(
            tx_id=tx_id,
            coordinator=self,
            isolation_level=isolation_level,
            timeout_seconds=timeout_seconds,
        )

    async def tx_read_key(self, tx_id: str, key: str) -> Any:
        """Read key value from state machine."""
        if self.state_machine:
            return self.state_machine.get(key)
        return None

    async def prepare_transaction(
        self,
        tx_id: str,
        context: Optional[TransactionContext] = None,
    ) -> TxPrepareResult:
        """
        Phase 1 of 2PC: Verify locks, check OCC version fencing, buffer write sets,
        and log prepare intent in WAL.
        """
        async with self._lock:
            tx = self._active_txs.get(tx_id)
            if not tx:
                return TxPrepareResult(
                    tx_id=tx_id,
                    participant_id=self.node_id,
                    vote_yes=False,
                    error_message=f"Transaction '{tx_id}' not found",
                )

            if context:
                tx.operations = list(context.operations)
                tx.read_set = dict(context.read_set)
                tx.write_set = dict(context.write_set)

            if tx.is_expired():
                tx.status = TxStatus.TIMED_OUT
                return TxPrepareResult(
                    tx_id=tx_id,
                    participant_id=self.node_id,
                    vote_yes=False,
                    error_message="Transaction lease expired before prepare",
                )

            # 1. Lock validation on write set
            for key in tx.write_set.keys():
                existing_holder = self._key_locks.get(key)
                if existing_holder and existing_holder != tx_id:
                    return TxPrepareResult(
                        tx_id=tx_id,
                        participant_id=self.node_id,
                        vote_yes=False,
                        error_message=f"Lock conflict on key '{key}': held by {existing_holder}",
                    )

            # 2. OCC version validation
            for op in tx.operations:
                if op.expected_version is not None:
                    curr_ver = self._key_versions.get(op.key, 0)
                    if op.expected_version != curr_ver:
                        return TxPrepareResult(
                            tx_id=tx_id,
                            participant_id=self.node_id,
                            vote_yes=False,
                            error_message=(
                                f"OCC version mismatch on key '{op.key}': "
                                f"expected {op.expected_version}, current {curr_ver}"
                            ),
                        )

            # 3. Grant locks and fencing tokens
            fencing_tokens: Dict[str, int] = {}
            for key in tx.write_set.keys():
                self._key_locks[key] = tx_id
                self._token_counter += 1
                self._fencing_tokens[key] = self._token_counter
                fencing_tokens[key] = self._token_counter

            tx.status = TxStatus.PREPARED
            tx.fencing_tokens = fencing_tokens

            if self.wal_engine:
                await self.wal_engine.append(
                    WALRecordType.TX_MARKER,
                    {
                        "action": "PREPARE",
                        "tx_id": tx_id,
                        "status": TxStatus.PREPARED.value,
                        "write_keys": list(tx.write_set.keys()),
                        "fencing_tokens": fencing_tokens,
                    },
                )

            logger.info(f"Transaction '{tx_id}' PREPARED successfully (keys={list(tx.write_set.keys())})")
            return TxPrepareResult(
                tx_id=tx_id,
                participant_id=self.node_id,
                vote_yes=True,
                fencing_tokens=fencing_tokens,
            )

    async def commit_transaction(
        self,
        tx_id: str,
        context: Optional[TransactionContext] = None,
    ) -> bool:
        """
        Phase 2 of 2PC: Execute atomic commit.
        Prepares if not already prepared, applies mutations to state machine,
        increments OCC versions, logs to WAL, and releases acquired locks.
        """
        async with self._lock:
            tx = self._active_txs.get(tx_id)
            if not tx:
                logger.error(f"Cannot commit unknown transaction '{tx_id}'")
                return False

        if tx.status != TxStatus.PREPARED:
            prep_res = await self.prepare_transaction(tx_id, context)
            if not prep_res.vote_yes:
                await self.rollback_transaction(tx_id, reason=prep_res.error_message)
                return False

        async with self._lock:
            # Apply all mutations to state machine
            if self.state_machine:
                for op in tx.operations:
                    if op.op_type == TxOperationType.SET:
                        self.state_machine._state[op.key] = op.value
                    elif op.op_type == TxOperationType.INCREMENT:
                        curr = self.state_machine.get(op.key, 0)
                        try:
                            self.state_machine._state[op.key] = int(curr) + op.delta
                        except (ValueError, TypeError):
                            self.state_machine._state[op.key] = op.delta
                    elif op.op_type == TxOperationType.DELETE:
                        self.state_machine._state.pop(op.key, None)

                    # Bump key OCC version
                    self._key_versions[op.key] = self._key_versions.get(op.key, 0) + 1

            # Release all locks held by this tx
            for key in tx.write_set.keys():
                if self._key_locks.get(key) == tx_id:
                    self._key_locks.pop(key, None)

            tx.status = TxStatus.COMMITTED
            tx.completed_at = time.time()
            self.total_committed += 1

        if self.wal_engine:
            await self.wal_engine.append(
                WALRecordType.TX_MARKER,
                {
                    "action": "COMMIT",
                    "tx_id": tx_id,
                    "status": TxStatus.COMMITTED.value,
                    "ops_count": len(tx.operations),
                    "operations": [op.to_dict() for op in tx.operations],
                    "write_set": dict(tx.write_set),
                },
            )

        logger.info(f"Transaction '{tx_id}' COMMITTED successfully ({len(tx.operations)} ops applied)")
        return True

    async def rollback_transaction(
        self,
        tx_id: str,
        reason: Optional[str] = None,
    ) -> bool:
        """
        Phase 2 Abort of 2PC: Release all locks, discard mutations, and log abort.
        """
        async with self._lock:
            tx = self._active_txs.get(tx_id)
            if not tx:
                return False

            # Release all locks held by this tx
            for key in tx.write_set.keys():
                if self._key_locks.get(key) == tx_id:
                    self._key_locks.pop(key, None)

            tx.status = TxStatus.ABORTED
            tx.error_message = reason
            tx.completed_at = time.time()
            self.total_aborted += 1

        if self.wal_engine:
            await self.wal_engine.append(
                WALRecordType.TX_MARKER,
                {
                    "action": "ABORT",
                    "tx_id": tx_id,
                    "status": TxStatus.ABORTED.value,
                    "reason": reason,
                },
            )

        logger.warning(f"Transaction '{tx_id}' ABORTED: {reason}")
        return True

    def get_transaction(self, tx_id: str) -> Optional[TxRecord]:
        return self._active_txs.get(tx_id)

    def list_active_transactions(self) -> List[TxRecord]:
        now = time.time()
        return [tx for tx in self._active_txs.values() if not tx.is_expired(now)]

    def get_metrics(self) -> Dict[str, Any]:
        return {
            "total_started": self.total_started,
            "total_committed": self.total_committed,
            "total_aborted": self.total_aborted,
            "active_transactions": len(self._active_txs),
            "locked_keys_count": len(self._key_locks),
        }
