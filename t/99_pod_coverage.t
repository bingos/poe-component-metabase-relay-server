use Test::More;
eval "use Test::Pod::Coverage 1.00";
plan skip_all => "Test::Pod::Coverage 1.00 required for testing POD coverage" if $@;
plan skip_all => 'Set TEST_POD to enable pod tests.' unless $ENV{TEST_POD};
all_pod_coverage_ok( { also_private => [ qr/^(START|DELAY)$/ ] } );
