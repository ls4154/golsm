package impl

import "github.com/ls4154/golsm/db"

type internalFilterPolicy struct {
	user db.FilterPolicy
}

func newInternalFilterPolicy(user db.FilterPolicy) db.FilterPolicy {
	if user == nil {
		return nil
	}
	return &internalFilterPolicy{user: user}
}

func (p *internalFilterPolicy) Name() string {
	return p.user.Name()
}

func (p *internalFilterPolicy) AppendFilter(keys [][]byte, dst []byte) []byte {
	userKeys := make([][]byte, 0, len(keys))
	for _, key := range keys {
		if len(key) >= 8 {
			userKeys = append(userKeys, ExtractUserKey(key))
		} else {
			// Fallback to preserve behavior on malformed keys.
			userKeys = append(userKeys, key)
		}
	}

	return p.user.AppendFilter(userKeys, dst)
}

func (p *internalFilterPolicy) MightContain(key, filter []byte) bool {
	if len(key) >= 8 {
		return p.user.MightContain(ExtractUserKey(key), filter)
	}
	return p.user.MightContain(key, filter)
}
