package gc

import (
	"context"
	"errors"
	"fmt"

	bstore "github.com/ipfs/go-ipfs/blocks/blockstore"
	dag "github.com/ipfs/go-ipfs/merkledag"
	pin "github.com/ipfs/go-ipfs/pin"

	logging "gx/ipfs/QmSpJByNKFX1sCsHBEp3R73FL4NF6FnQTEGyNAXHm2GS52/go-log"
	cid "gx/ipfs/QmV5gPoRsjN1Gid3LMdNZTyfCtP2DsvqEbMAmz82RmmiGk/go-cid"
	node "gx/ipfs/QmYDscK7dmdo2GZ9aumS8s5auUUAH5mR1jvj5pYhWusfK7/go-ipld-node"
)

var log = logging.Logger("gc")

// GC performs a mark and sweep garbage collection of the blocks in the blockstore
// first, it creates a 'marked' set and adds to it the following:
// - all recursively pinned blocks, plus all of their descendants (recursively)
// - bestEffortRoots, plus all of its descendants (recursively)
// - all directly pinned blocks
// - all blocks utilized internally by the pinner
//
// The routine then iterates over every block in the blockstore and
// deletes any block that is not found in the marked set.
//
func GC(ctx context.Context, bs bstore.GCBlockstore, ls dag.LinkService, pn pin.Pinner, bestEffortRoots []*cid.Cid) (<-chan *cid.Cid, <-chan error) {
	unlocker := bs.GCLock()
	ls = ls.GetOfflineLinkService()

	output := make(chan *cid.Cid)
	errOutput := make(chan error)

	go func() {
		defer close(errOutput)
		defer close(output)
		defer unlocker.Unlock()

		gcs, err := ColoredSet(ctx, pn, ls, bestEffortRoots, errOutput)
		if err != nil {
			errOutput <- err
			return
		}

		keychan, err := bs.AllKeysChan(ctx)
		if err != nil {
			errOutput <- err
			return
		}

		errors := false

		for {
			select {
			case k, ok := <-keychan:
				if !ok {
					return
				}
				if !gcs.Has(k) {
					err := bs.DeleteBlock(k)
					if err != nil {
						errors = true
						errOutput <- &CoultNotDeleteBlockError{k, err}
						//log.Debugf("Error removing key from blockstore: %s", err)
						// continue as error is non-fatal
					}
					select {
					case output <- k:
					case <-ctx.Done():
						return
					}
				}
			case <-ctx.Done():
				return
			}
		}
		if errors {
			errOutput <- CouldNotDeleteSomeBlocksError
		}
	}()

	return output, errOutput
}

func Descendants(ctx context.Context, getLinks dag.GetLinks, set *cid.Set, roots []*cid.Cid) error {
	for _, c := range roots {
		set.Add(c)

		// EnumerateChildren recursively walks the dag and adds the keys to the given set
		err := dag.EnumerateChildren(ctx, getLinks, c, set.Visit)
		if err != nil {
			return err
		}
	}

	return nil
}

func ColoredSet(ctx context.Context, pn pin.Pinner, ls dag.LinkService, bestEffortRoots []*cid.Cid, errOutput chan<- error) (*cid.Set, error) {
	// KeySet currently implemented in memory, in the future, may be bloom filter or
	// disk backed to conserve memory.
	errors := false
	gcs := cid.NewSet()
	getLinks := func(ctx context.Context, cid *cid.Cid) ([]*node.Link, error) {
		links, err := ls.GetLinks(ctx, cid)
		if err != nil {
			errors = true
			errOutput <- &CoultNotFetchLinksError{cid, err}
		}
		return links, nil
	}
	err := Descendants(ctx, getLinks, gcs, pn.RecursiveKeys())
	if err != nil {
		errors = true
		errOutput <- err
	}

	bestEffortGetLinks := func(ctx context.Context, cid *cid.Cid) ([]*node.Link, error) {
		links, err := ls.GetLinks(ctx, cid)
		if err != nil && err != dag.ErrNotFound {
			errors = true
			errOutput <- &CoultNotFetchLinksError{cid, err}
		}
		return links, nil
	}
	err = Descendants(ctx, bestEffortGetLinks, gcs, bestEffortRoots)
	if err != nil {
		errors = true
		errOutput <- err
	}

	for _, k := range pn.DirectKeys() {
		gcs.Add(k)
	}

	err = Descendants(ctx, getLinks, gcs, pn.InternalPins())
	if err != nil {
		errors = true
		errOutput <- err
	}

	if errors {
		return nil, CoundNotFetchAllLinksError
	} else {
		return gcs, nil
	}
}

var CoundNotFetchAllLinksError = errors.New("could not retrieve some links, aborting")

var CouldNotDeleteSomeBlocksError = errors.New("could not delete some blocks")

type CoultNotFetchLinksError struct {
	Key *cid.Cid
	Err error
}

func (e *CoultNotFetchLinksError) Error() string {
	return fmt.Sprintf("could not retrieve links for %s: %s", e.Key, e.Err)
}

type CoultNotDeleteBlockError struct {
	Key *cid.Cid
	Err error
}

func (e *CoultNotDeleteBlockError) Error() string {
	return fmt.Sprintf("could not remove %s: %s", e.Key, e.Err)
}
