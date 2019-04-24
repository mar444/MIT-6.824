package shardmaster

import "log"

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
    if Debug > 0 {
        log.Printf(format, a...)
    }
    return
}

func LPrintf(format string, a ...interface{}) (n int, err error) {
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

func remove(s []chan AppliedResult, i int) []chan AppliedResult {
    s[len(s)-1], s[i] = s[i], s[len(s)-1]
    return s[:len(s)-1]
}

func removeGroup(ids[]int, target int) []int {
    result := make([]int, 0)
    for _, id := range ids {
        if id != target {
            result = append(result, id)
        }
    }
    return result
}

func getGroupIds(groups map[int][]string) []int {
    groupIds := make([]int, 0)
    for gid, _ := range groups {
        groupIds = append(groupIds, gid)
    }
    return groupIds
}