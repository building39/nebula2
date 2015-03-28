%% @author mmartin
%% @doc @todo Add description to nebula2_bootstrap.

-module(nebula2_bootstrap).

-include("include/nebula.hrl").

%% ====================================================================
%% API functions
%% ====================================================================
-export([bootstrap/0]).

%% @doc Bootstraps the CDMI root and capabilities containers
-spec nebula2_bootstrap:bootstrap() -> {ok, string()}.
bootstrap() ->
    {ok, Pid} = riakc_pb_socket:start_link("nebriak1", 8087),
    lager:debug("Riak PID is ~p", [Pid]),
    Tstamp = nebula2_utils:get_time(),
    Location = nebula2_app:riak_location(),
    lager:info("Location: ~p", [Location]),

    lager:info("Bootstrapping the CDMI root container:"),
    RootKey = nebula2_utils:make_key(),
    {ok, RootDoc} = cdmiroot_dtl:render([
                                 {content_type, "application/cdmi-container"},
                                 {key, RootKey},
                                 {location, Location},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, RootKey, RootDoc),

    lager:info("Bootstrapping 2nd level objects..."),
    lager:info("Bootstrapping the CDMI capabilities container:"),
    CapabilitiesKey = nebula2_utils:make_key(),
    {ok, CapabilitiesDoc} = capabilities_dtl:render([
                                 {content_type, "application/cdmi-capability"},
                                 {key, CapabilitiesKey},
                                 {location, Location},
                                 {parent_id, RootKey},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, CapabilitiesKey, CapabilitiesDoc),

    lager:info("Bootstrapping the CDMI domains container:"),
    DomainsKey = nebula2_utils:make_key(),
    {ok, DomainsDoc} = domains_dtl:render([
                                 {content_type, "application/cdmi-domains"},
                                 {key, DomainsKey},
                                 {location, Location},
                                 {parent_id, RootKey},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, DomainsKey, DomainsDoc),

    lager:info("Bootstrapping the CDMI system configuration container:"),
    SysconfigKey = nebula2_utils:make_key(),
    {ok, SysconfigDoc} = sysconfig_dtl:render([
                                 {content_type, "application/cdmi-container"},
                                 {key, SysconfigKey},
                                 {location, Location},
                                 {parent_id, RootKey},
                                 {key, SysconfigKey},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, SysconfigKey, SysconfigDoc),

    lager:info("Bootstrapping 3rd level objects..."),
    lager:info("Bootstrapping the CDMI container capabilities container:"),
    ContainerCapabilitiesKey = nebula2_utils:make_key(),
    {ok, ContainerCapabilitiesDoc} = container_capabilities_dtl:render([
                                 {content_type, "application/cdmi-capability"},
                                 {key, ContainerCapabilitiesKey},
                                 {location, Location},
                                 {parent_id, CapabilitiesKey},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, ContainerCapabilitiesKey, ContainerCapabilitiesDoc),

    lager:info("Bootstrapping the CDMI dataobject capabilities container:"),
    DataobjectCapabilitiesKey = nebula2_utils:make_key(),
    {ok, DataobjectCapabilitiesDoc} = dataobject_capabilities_dtl:render([
                                 {content_type, "application/cdmi-capability"},
                                 {key, DataobjectCapabilitiesKey},
                                 {location, Location},
                                 {parent_id, CapabilitiesKey},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, DataobjectCapabilitiesKey, DataobjectCapabilitiesDoc),

    lager:info("Bootstrapping the CDMI domain capabilities container:"),
    DomainCapabilitiesKey = nebula2_utils:make_key(),
    {ok, DomainCapabilitiesDoc} = domain_capabilities_dtl:render([
                                 {content_type, "application/cdmi-capability"},
                                 {key, DomainCapabilitiesKey},
                                 {location, Location},
                                 {parent_id, CapabilitiesKey},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, DomainCapabilitiesKey, DomainCapabilitiesDoc),

    lager:info("Bootstrapping the CDMI queue capabilities container:"),
    QueueCapabilitiesKey = nebula2_utils:make_key(),
    {ok, QueueCapabilitiesDoc} = queue_capabilities_dtl:render([
                                 {content_type, "application/cdmi-capability"},
                                 {key, QueueCapabilitiesKey},
                                 {location, Location},
                                 {parent_id, CapabilitiesKey},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, QueueCapabilitiesKey, QueueCapabilitiesDoc),

    lager:info("Bootstrapping the CDMI system domain container:"),
    SystemDomainKey = nebula2_utils:make_key(),
    {ok, SystemDomainDoc} = systemdomain_dtl:render([
                                 {content_type, "application/cdmi-domain"},
                                 {key, SystemDomainKey},
                                 {location, Location},
                                 {parent_id, DomainsKey},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, SystemDomainKey, SystemDomainDoc),

    lager:info("Bootstrapping the CDMI storage subsystem container:"),
    StorageSubsystemKey = nebula2_utils:make_key(),
    lager:info("Storage subsystem key: ~p", [StorageSubsystemKey]),
    {ok, StorageSubsystemDoc} = storage_subsystems_dtl:render([
                                 {key, StorageSubsystemKey},
                                 {location, Location},
                                 {parent_id, SysconfigKey},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, StorageSubsystemKey, StorageSubsystemDoc),

    lager:info("Bootstrapping 4th level objects..."),
    lager:info("Bootstrapping the CDMI permanent capabilities container:"),
    PermanentCapabilitiesKey = nebula2_utils:make_key(),
    {ok, PermanentCapabilitiesDoc} = permanent_capabilities_dtl:render([
                                 {content_type, "application/cdmi-capability"},
                                 {key, PermanentCapabilitiesKey},
                                 {location, Location},
                                 {parent_id, ContainerCapabilitiesKey},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, PermanentCapabilitiesKey, PermanentCapabilitiesDoc),

    lager:info("Bootstrapping the CDMI dataobject configuration capabilities:"),
    DOConfigurationKey = nebula2_utils:make_key(),
    {ok, DOConfigurationDoc} = dataobject_configuration_capabilities_dtl:render([
                                 {content_type, "application/cdmi-capability"},
                                 {key, DOConfigurationKey},
                                 {location, Location},
                                 {parent_id, DataobjectCapabilitiesKey},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, DOConfigurationKey, DOConfigurationDoc),

    lager:info("Bootstrapping the CDMI dataobject member capabilities:"),
    DOMemberKey = nebula2_utils:make_key(),
    {ok, DOMemberDoc} = dataobject_member_capabilities_dtl:render([
                                 {content_type, "application/cdmi-capability"},
                                 {key, DOMemberKey},
                                 {location, Location},
                                 {parent_id, DataobjectCapabilitiesKey},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, DOMemberKey, DOMemberDoc),

    lager:info("Bootstrapping the CDMI dataobject versions capabilities:"),
    DOVersionsKey = nebula2_utils:make_key(),
    {ok, DOVersionsDoc} = dataobject_versions_capabilities_dtl:render([
                                 {content_type, "application/cdmi-capability"},
                                 {key, DOVersionsKey},
                                 {location, Location},
                                 {parent_id, DataobjectCapabilitiesKey} ]),   
    insert(Pid, DOVersionsKey, DOVersionsDoc),

    lager:info("Bootstrapping the CDMI domain members container:"),
    DomainMembersKey = nebula2_utils:make_key(),
    {ok, DomainMembersDoc} = domain_members_dtl:render([
                                 {content_type, "application/cdmi-container"},
                                 {key, DomainMembersKey},
                                 {location, Location},
                                 {parent_id, SystemDomainKey},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, DomainMembersKey, DomainMembersDoc),

    lager:info("Bootstrapping the CDMI domain summary container:"),
    DomainSummaryKey = nebula2_utils:make_key(),
    {ok, DomainSummaryDoc} = domain_summary_dtl:render([
                                 {content_type, "application/cdmi-container"},
                                 {key, DomainSummaryKey},
                                 {location, Location},
                                 {parent_id, SystemDomainKey},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, DomainSummaryKey, DomainSummaryDoc),

    lager:info("Bootstrapping 5th level objects..."),
    lager:info("Bootstrapping the CDMI dataobject permanent capabilities:"),
    DOPermanentCapabilitiesKey = nebula2_utils:make_key(),
    {ok, DOPermanentCapabilitiesDoc} = dataobject_configuration_capabilities_permanent_dtl:render([
                                 {content_type, "application/cdmi-capability"},
                                 {key, DOPermanentCapabilitiesKey},
                                 {location, Location},
                                 {parent_id, DOConfigurationKey},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, DOPermanentCapabilitiesKey, DOPermanentCapabilitiesDoc),

    lager:info("Bootstrapping the CDMI dataobject config version capabilities:"),
    DOConfigVersionKey = nebula2_utils:make_key(),
    {ok, DOConfigVersionDoc} = dataobject_configuration_capabilities_version_dtl:render([
                                 {content_type, "application/cdmi-capability"},
                                 {key, DOConfigVersionKey},
                                 {location, Location},
                                 {parent_id, DOConfigurationKey},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, DOConfigVersionKey, DOConfigVersionDoc),

    lager:info("Bootstrapping the CDMI administrator member:"),
    AdminMemberKey = nebula2_utils:make_key(),
    {ok, AdminMemberDoc} = administrator_dtl:render([
                                 {content_type, "application/cdmi-object"},
                                 {key, AdminMemberKey},
                                 {location, Location},
                                 {parent_id, DomainMembersKey},
                                 {tstamp, Tstamp} ]),   
    insert(Pid, AdminMemberKey, AdminMemberDoc),

    lager:info("Bootstrapping the CDMI cumulative summary:"),
    CumSummaryKey = nebula2_utils:make_key(),
    {ok, CumSummaryDoc} = summary_cumulative_dtl:render([
                                 {content_type, "application/cdmi-object"},
                                 {key, CumSummaryKey},
                                 {name, "cumulative/"},
                                 {parent_id, DomainSummaryKey}
                                ]),
    insert(Pid, CumSummaryKey, CumSummaryDoc),

    lager:info("Bootstrapping the CDMI current summary:"),
    CurSummaryKey = nebula2_utils:make_key(),
    {ok, CurSummaryDoc} = summary_current_dtl:render([
                                 {content_type, "application/cdmi-object"},
                                 {key, CurSummaryKey},
                                 {name, "current"},
                                 {parent_id, DomainSummaryKey}
                                ]),
    insert(Pid, CurSummaryKey, CurSummaryDoc),

    lager:info("Bootstrapping the CDMI daily summary container:"),
    DailySummaryKey = nebula2_utils:make_key(),
    {ok, DailySummaryDoc} = summary_dtl:render([
                                 {content_type, "application/cdmi-container"},
                                 {key, DailySummaryKey},
                                 {name, "daily/"},
                                 {parent_id, DomainSummaryKey}
                                ]),
    insert(Pid, DailySummaryKey, DailySummaryDoc),

    lager:info("Bootstrapping the CDMI monthly summary container:"),
    MonthlySummaryKey = nebula2_utils:make_key(),
    {ok, MonthlySummaryDoc} = summary_dtl:render([
                                 {content_type, "application/cdmi-container"},
                                 {key, MonthlySummaryKey},
                                 {name, "monthly/"},
                                 {parent_id, DomainSummaryKey}
                                ]),
    insert(Pid, MonthlySummaryKey, MonthlySummaryDoc),

    lager:info("Bootstrapping the CDMI yearly summary container:"),
    YearlySummaryKey = nebula2_utils:make_key(),
    {ok, YearlySummaryDoc} = summary_dtl:render([
                                 {content_type, "application/cdmi-container"},
                                 {key, YearlySummaryKey},
                                 {name, "yearly/"},
                                 {parent_id, DomainSummaryKey}
                                ]),
    insert(Pid, YearlySummaryKey, YearlySummaryDoc),
    {ok, "System bootstrapped successfully"}.

%% ====================================================================
%% Internal functions
%% ====================================================================
insert(Pid, Key, Doc) ->
    ok = file:write_file("/tmp/json.txt", Doc),
    Json = jsx:minify(list_to_binary(Doc)),
    nebula2_riak:put(Pid, Key, Json).

