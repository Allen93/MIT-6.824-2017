package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func IsVoteLeaderPass(term int, total int, replies []*RequestVoteReply) bool {
	passCount := 0
	for _, reply := range replies {
		if reply.VoteGrant && reply.Term == term {
			passCount++
		}
	}
	return passCount*2 >= total
}

func IsLeaderHeartBeatPass(term int, total int, replies []*AppendEntriesReply) bool {
	passCount := 0
	for _, reply := range replies {
		if reply.Success && reply.Term == term {
			passCount++
		}
	}
	return passCount*2 >= total
}
