package raft

type Log struct {
    Data       map[int]LogEntry
    LastIndex  int
}

func (log *Log) GetFirstIndex() int {
    return log.LastIndex - len(log.Data) + 1
}

func (log *Log) GetLastIndex(snapshot Snapshot) int {
    if log.LastIndex == 0 && (snapshot.State != nil) && snapshot.LastIndex != 0 {
        return snapshot.LastIndex
    } else {
        return log.LastIndex
    }
}

func (log *Log) ContainsKey(key int) bool {
    return key >= log.GetFirstIndex() && key <= log.LastIndex
}

func (log *Log) Get(key int) LogEntry {
    return log.Data[key]
}

func (log *Log) GetRange(startIdx, endIdx int) []LogEntry {
    var entries []LogEntry
    for i := startIdx; i <= endIdx; i++ {
        entries = append(entries, log.Get(i))
    }
    return entries
}

func (log *Log) Put(index int, entry LogEntry) {
    log.Data[index] = entry
    log.LastIndex = index
}

func (log *Log) RemoveRange(startIdx, endIdx int) {
    for i := startIdx; i <= endIdx; i++ {
        delete(log.Data, i)
    }

    if len(log.Data) == 0 {
        log.LastIndex = 0
    } else if endIdx == log.LastIndex {
        log.LastIndex = startIdx - 1
    }

    DPrintf("Removed log from %v to %v LastIndex %v\n", startIdx, endIdx, log.LastIndex)
}

func (log *Log) GetLastLogInfo(endIdx int, snapshot Snapshot) (int, int) {
    if endIdx < 0 {
        return 0, 0
    } else if endIdx == 0 && snapshot.LastIndex != 0 {
        return snapshot.LastIndex, snapshot.LastTerm
    }

    lastLog := log.Get(endIdx)
    return lastLog.Index, lastLog.Term
}