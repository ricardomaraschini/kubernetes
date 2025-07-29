package placementrequest

// Option sets an option for a PlacementRequest controller.
type Option func(*options)

// options holds the options for a PlacementRequest controller.
type options struct{}

// defaultOptions holds the default options for a PlacementRequest controller.
var defaultOptions = options{}
