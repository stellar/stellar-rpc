package event

// ColdDirs are the two bucket directories a chunk's cold events artifacts
// live in, composed by geometry.Layout. They are separate roots because the
// pack is streamed sequentially and holds almost all of the bytes while
// index.pack is probed at random, so a deployment can give the probes the
// IOPS they need without moving the pack with them.
//
// ValidateRoots rejects two storage roots resolving to one path, so in a
// deployment these always differ. Tests may point both at one directory.
type ColdDirs struct {
	Data  string
	Index string
}
