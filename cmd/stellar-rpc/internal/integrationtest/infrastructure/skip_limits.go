package infrastructure

// SkipLimitsUpgrade returns the value to pass as TestConfig.ApplyLimits when a
// test does not need Core's Soroban resource limits raised. Skipping the
// upgrade saves about 19 seconds of setup.
//
//	test := infrastructure.NewTest(t, &infrastructure.TestConfig{
//		ApplyLimits: infrastructure.SkipLimitsUpgrade(),
//	})
//
// Only a test that never submits a Soroban transaction may skip it. Anything
// that uploads a contract, invokes one, or calls simulateTransaction needs the
// raised limits and must leave ApplyLimits unset.
func SkipLimitsUpgrade() *string {
	skip := ""
	return &skip
}
