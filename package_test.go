// Copyright 2014 Canonical Ltd.
// Licensed under the LGPLv3, see LICENCE file for details.

package txn_test

import (
	stdtesting "testing"

	"github.com/juju/testing"
)

func Test(t *stdtesting.T) {
	testing.MgoTestPackage(t, nil)
}
