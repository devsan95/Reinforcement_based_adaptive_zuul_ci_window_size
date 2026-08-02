*** Settings ***
Documentation    Generate mixed high-volume Gerrit/Zuul traffic for RL/TCP window-size behavior testing.
Library    OperatingSystem
Library    ../libraries/zuul_load_keywords.py
Suite Setup    Prepare Workspace    ${GERRIT_URL}    ${GERRIT_AUTH}    ${PROJECTS}    ${BRANCHES}    ${WORKSPACE_DIR}
Suite Teardown    Cleanup Workspace

*** Variables ***
${GERRIT_URL}    http://localhost:8080
${ZUUL_URL}    http://localhost:19090
${GERRIT_AUTH}    admin:secret
${TENANT}    example-tenant
${CHECK_PIPELINE}    check
${GATE_PIPELINE}    gate
${PROJECTS}    test1
${BRANCHES}    master
${TOTAL_COUNT}    300
${PACE_MS}    150
${SCENARIO_MIX}    burst=45,trickle=35,patchset=15,recheck=5
${GATE_RATIO}    0.35
${SEED}    101
${STATUS_SAMPLE_INTERVAL}    25
${ARTIFACT_DIR}    ${OUTPUT DIR}${/}window-size-load
${WORKSPACE_DIR}

*** Test Cases ***
Generate Mixed Load For RL Window Analysis
    [Documentation]    Creates hundreds of mixed events and writes artifacts for RL window audit/throughput correlation.
    Set Environment Variable    ZUUL_URL    ${ZUUL_URL}
    ${summary}=    Run Mixed Load
    ...    total_count=${TOTAL_COUNT}
    ...    pace_ms=${PACE_MS}
    ...    scenario_mix=${SCENARIO_MIX}
    ...    tenant=${TENANT}
    ...    check_pipeline=${CHECK_PIPELINE}
    ...    gate_pipeline=${GATE_PIPELINE}
    ...    gate_ratio=${GATE_RATIO}
    ...    artifact_dir=${ARTIFACT_DIR}
    ...    seed=${SEED}
    ...    status_sample_interval=${STATUS_SAMPLE_INTERVAL}
    Log To Console    Summary file: ${summary}
