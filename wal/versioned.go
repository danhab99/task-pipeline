package wal

type VersionedWal struct {
	set WalSet
}

func NewVersionedWal(dir string) VersionedWal {
	return VersionedWal{
		set: NewWalSet(dir),
	}
}

func (s VersionedWal) Close() {
	s.set.Close()
}

type IDed interface {
	ID() string
}

func (s VersionedWal) Get(i IDed) IndexedWriteAheadLog {
	return s.set.Get(i.ID())
}

func (s VersionedWal) Keys() []string {
	return s.set.Keys()
}
