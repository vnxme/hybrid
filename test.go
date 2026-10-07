package hybrid

type pConfig struct{}

type POption interface {
	apply(cfg *pConfig)
}

func P(opts ...POption) {
	var cfg pConfig
	for _, opt := range opts {
		opt.apply(&cfg)
	}
}

type pOptionFunc func(*pConfig)

func (f pOptionFunc) apply(cfg *pConfig) {
	f(cfg)
}

func WithSomething() POption {
	return pOptionFunc(func(cfg *pConfig) {
		// modify cfg here
	})
}
