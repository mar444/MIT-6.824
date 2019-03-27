package raftkv
import "log"

const Debug = 2

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        log.Printf(format, a...)
    }
    return
}

func LPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug == 1 {
        log.Printf(format, a...)
    }
    return
}

func MaxInt(a, b int) int {
    if a > b {
        return a
    } else {
        return b
    }
}

func remove(s []chan AppliedResult, i int) []chan AppliedResult {
    s[len(s)-1], s[i] = s[i], s[len(s)-1]
    return s[:len(s)-1]
}