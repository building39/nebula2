PROJECT = nebula2
DEPS = lager cowboy riakc pooler
dep_pooler = git git@github.com:seth/pooler.git 1.4.0
dep_riakc = git git@github.com:basho/riak-erlang-client.git 2.1.0.1
include erlang.mk

# Turn on lager
ERLC_COMPILE_OPTS= +'{parse_transform, lager_transform}'

ERLC_OPTS += $(ERLC_COMPILE_OPTS)
TEST_ERLC_OPTS += $(ERLC_COMPILE_OPTS)
