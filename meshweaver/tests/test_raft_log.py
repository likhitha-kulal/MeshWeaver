"""
Unit tests for RaftLog storage, indexing, conflict truncation, and commit advancement.
"""

import pytest
from meshweaver.models import LogEntry, RaftCommandType
from meshweaver.raft_log import RaftLog


def test_raft_log_empty():
    log = RaftLog()
    assert log.last_index == 0
    assert log.last_term == 0
    assert log.commit_index == 0
    assert log.last_applied == 0
    assert log.count == 0


def test_raft_log_append_and_lookup():
    log = RaftLog()
    e1 = log.append_command(term=1, command_type=RaftCommandType.SET, key="k1", value="v1")
    assert e1.index == 1
    assert log.last_index == 1
    assert log.last_term == 1
    assert log.get_entry(1).key == "k1"

    e2 = log.append_command(term=1, command_type=RaftCommandType.SET, key="k2", value="v2")
    assert e2.index == 2
    assert log.last_index == 2
    assert log.get_term(2) == 1

    entries = log.slice_from(1)
    assert len(entries) == 2
    assert entries[0].index == 1
    assert entries[1].index == 2


def test_raft_log_consistency_check():
    log = RaftLog()
    log.append_command(term=1, command_type=RaftCommandType.SET, key="k1", value="v1")
    log.append_command(term=2, command_type=RaftCommandType.SET, key="k2", value="v2")

    assert log.check_consistency(0, 0) is True
    assert log.check_consistency(1, 1) is True
    assert log.check_consistency(2, 2) is True
    assert log.check_consistency(2, 1) is False
    assert log.check_consistency(3, 2) is False


def test_raft_log_reconcile_and_truncate_conflicts():
    log = RaftLog()
    log.append_command(term=1, command_type=RaftCommandType.SET, key="k1", value="v1")
    log.append_command(term=1, command_type=RaftCommandType.SET, key="k2", value="v2")
    log.append_command(term=1, command_type=RaftCommandType.SET, key="k3", value="v3")

    # Follower has 3 entries from term 1. Leader sends entry at index 3 with term 2.
    new_entries = [
        LogEntry(index=3, term=2, command_type=RaftCommandType.SET, key="k3_new", value="v3_new"),
        LogEntry(index=4, term=2, command_type=RaftCommandType.SET, key="k4", value="v4"),
    ]
    success, match_idx = log.reconcile_follower_entries(prev_log_index=2, prev_log_term=1, new_entries=new_entries)
    assert success is True
    assert match_idx == 4
    assert log.last_index == 4
    assert log.get_entry(3).value == "v3_new"
    assert log.get_entry(3).term == 2
    assert log.get_entry(4).key == "k4"


def test_raft_log_commit_and_apply():
    log = RaftLog()
    for i in range(1, 6):
        log.append_command(term=1, command_type=RaftCommandType.SET, key=f"k{i}", value=f"v{i}")

    assert log.commit_index == 0
    log.advance_commit_index(3)
    assert log.commit_index == 3

    unapplied = log.get_unapplied_entries()
    assert len(unapplied) == 3
    assert [e.index for e in unapplied] == [1, 2, 3]

    log.mark_applied(3)
    assert log.last_applied == 3
    assert log.get_unapplied_entries() == []


def test_raft_log_snapshot_compaction():
    log = RaftLog()
    for i in range(1, 11):
        log.append_command(term=1, command_type=RaftCommandType.SET, key=f"k{i}", value=f"v{i}")
    log.advance_commit_index(8)
    log.mark_applied(8)

    log.compact_log_before(5)
    assert log.last_index == 10
    assert log.get_entry(5) is None
    assert log.get_entry(6).index == 6
