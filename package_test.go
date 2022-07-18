// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn_test

import (
	stdtesting "testing"

	mgotesting "github.com/juju/mgo/v2/testing"
)

func Test(t *stdtesting.T) {
	// Pass nil for Certs because we don't need SSL
	mgotesting.MgoTestPackage(t, nil)
}
