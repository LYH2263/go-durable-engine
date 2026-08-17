package webui

import _ "embed"

//go:embed web/index.html
var indexHTML []byte

// IndexHTML returns the embedded status page.
func IndexHTML() []byte { return indexHTML }
