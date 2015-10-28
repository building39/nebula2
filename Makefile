PROJECT = nebula2
DEPS = lager cowboy crc16 jsx uuid riakc folsom pooler mcd cuttlefish
dep_lager = git git@github.com:basho/lager.git 2.1.1
dep_cowboy = git git@github.com:ninenines/cowboy.git 1.0.3
dep_crc16 = git git@github.com:building39/crc16_nif.git 1.1
dep_jsx = git git@github.com:talentdeficit/jsx.git master
dep_folsom = git git@github.com:boundary/folsom.git master
dep_pooler = git git@github.com:seth/pooler.git master
dep_riakc = git git@github.com:basho/riak-erlang-client.git master
dep_uuid = git git://github.com/avtobiff/erlang-uuid.git v0.4.7
dep_mcd = git git@github.com:EchoTeam/mcd.git master
dep_cuttlefish = git git@github.com:basho/cuttlefish.git 2.0
include erlang.mk

# Turn on lager
ERLC_COMPILE_OPTS= +'{parse_transform, lager_transform}'

ERLC_OPTS += $(ERLC_COMPILE_OPTS)
TEST_ERLC_OPTS += $(ERLC_COMPILE_OPTS)
