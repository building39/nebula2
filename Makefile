PROJECT = nebula2
# DEPS = lager meck crc16 jsx uuid riakc folsom_cowboy folsom cowboy pooler mcd
DEPS = lager meck crc16 jsx uuid riakc cowboy pooler mcd
dep_lager = git http://github.com/basho/lager.git 2.1.1
dep_meck = git http://github.com/eproxus/meck.git 0.8.2
dep_crc16 = git http://github.com/building39/crc16_nif.git 1.1
dep_jsx = git http://github.com/talentdeficit/jsx.git master
dep_uuid = git git://github.com/avtobiff/erlang-uuid.git v0.4.7
dep_riakc = git http://github.com/basho/riak-erlang-client.git master
#dep_folsom_cowboy = git http://github.com/building39/folsom_cowboy.git master
#dep_folsom = git http://github.com/building39/folsom.git master
dep_cowboy = git http://github.com/ninenines/cowboy.git 1.0.3
dep_pooler = git http://github.com/seth/pooler.git master
dep_mcd = git http://github.com/building39/mcd.git master
include erlang.mk

# Turn on lager
ERLC_COMPILE_OPTS= +'{parse_transform, lager_transform}'

ERLC_OPTS += $(ERLC_COMPILE_OPTS)
TEST_ERLC_OPTS += $(ERLC_COMPILE_OPTS)
