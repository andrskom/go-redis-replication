package resp

type CmdName string
type ConfigKey string

const (
	CmdDel     CmdName = "del"
	CmdLPush   CmdName = "lpush"
	CmdSelect  CmdName = "select"
	CmdSet     CmdName = "set"
	CmdSetex   CmdName = "setex"
	CmdSync    CmdName = "sync"
	CmdFlushDB CmdName = "flushdb"
	CmdHset    CmdName = "hset"
	CmdConfig  CmdName = "config"

	ConfigSubCmdSet = "set"
	ConfigSubCmdGet = "get"

	ConfigKeyHashMaxZiplistValue ConfigKey = "hash-max-ziplist-value"
	ConfigKeyHashMaxZiplistEntries ConfigKey = "hash-max-ziplist-entries"
)

type Cmd []string

func NewCmd(cmd CmdName, args ...string) Cmd {
	return append([]string{string(cmd)}, args...)
}
