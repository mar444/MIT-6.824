package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
    log.SetFlags(log.LstdFlags | log.Lmicroseconds)
    if Debug > 0 {
        log.Printf(format, a...)
    }
    return
}

func LPrintf(format string, a ...interface{}) (n int, err error) {
    log.SetFlags(log.LstdFlags | log.Lmicroseconds)
    if Debug == -1 {
        log.Printf(format, a...)
    }
    return
}

func MinInt(a, b int) int {
    if a > b {
        return b
    } else {
        return a
    }
}

func MaxInt(a, b int) int {
    if a > b {
        return a
    } else {
        return b
    }
}
