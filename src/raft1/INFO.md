## GenAI Usage in raft.go
Claude 4.5 was asked to identify and explain specific behaviour in edge case scenarios surrounding the RAFT protocol
Cursor was asked to amend draft election operations to be able to handle re-election backoff in the case of split votes
Cursor was asked to simplify the draft data structures for the rpc call arguments to be more effective
Cursor was asked to rearrange the election timeout logic to fit the ReadME.md hints (moved timeouts and heartbeat update calls)
ChatGPT was asked to help debug appendEntry append to follower log logic after consistency checks
ChatGPT was asked to amend leader back off algorithm to implement the optimization described in the RAFT paper
ChatGPT was asked to identify areas of redundant RPC calls to increase performance


## Team Member Contribution
Bryan Zhou - 16079717 - Collaboratively worked on 3A implementation, responsible for 3D implementation, code reviews for 3B, 3C
Matthew Cai - # student number - Collaboratively worked on 3A implementation, responsible for 3B, 3C implementation, code reviews for 3A, 3D