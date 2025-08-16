package change_arb

default assessment_required := false
default assessment_mandatory := false
default review_mode := "Express to prod"

arb_domains := ordered_domains(arb_domains_set)
fired_rules := [r | rule_messages[r]]

########################################
# Crown Jewels (A)
########################################
assessment_required if { input.criticality == "A" }
assessment_mandatory if { input.criticality == "A" }

rule_messages["Criticality A → Full review + EA + Security + Data + Service Transition"] if {
    input.criticality == "A"
}

########################################
# Criticality B
########################################
assessment_required if { input.criticality == "B" }

assessment_mandatory if {
    input.criticality == "B"
    b_mandatory_trigger
}

arb_domains_set["Security"] if { input.criticality == "B"; input.security == "A1" }
arb_domains_set["Security"] if { input.criticality == "B"; input.security == "A2" }
arb_domains_set["Data"]     if { input.criticality == "B"; input.integrity == "A" }
arb_domains_set["EA"]       if { input.criticality == "B"; input.availability == "A" }
arb_domains_set["Service Transition"] if { input.criticality == "B"; input.resilience == "A" }

rule_messages["Criticality B + security=A1/A2 or integrity/availability/resilience=A → Full review with relevant domain(s)"] if {
    input.criticality == "B"
    b_mandatory_trigger
}

# B non-mandatory branch
assessment_mandatory := false if {
    input.criticality == "B"
    not b_mandatory_trigger
    b_all_leq_b
}
arb_domains_set["EA"] if { input.criticality == "B"; not b_mandatory_trigger; b_all_leq_b }
rule_messages["Criticality B + all CIA+S ≤ B → Scoped review + EA only"] if {
    input.criticality == "B"
    not b_mandatory_trigger
    b_all_leq_b
}

########################################
# Criticality C
########################################
# C with Security A1 → Full + Security
assessment_required if { input.criticality == "C"; input.security == "A1" }
assessment_mandatory if { input.criticality == "C"; input.security == "A1" }
arb_domains_set["Security"] if { input.criticality == "C"; input.security == "A1" }
rule_messages["Criticality C + security=A1 → Full review + Security"] if {
    input.criticality == "C"
    input.security == "A1"
}

# C with Integrity A AND Resilience A → Full + Data + Service Transition
assessment_required if { input.criticality == "C"; integrity_and_resilience_a }
assessment_mandatory if { input.criticality == "C"; integrity_and_resilience_a }
arb_domains_set["Data"] if { input.criticality == "C"; integrity_and_resilience_a }
arb_domains_set["Service Transition"] if { input.criticality == "C"; integrity_and_resilience_a }
rule_messages["Criticality C + integrity=A and resilience=A → Full review + Data + Service Transition"] if {
    input.criticality == "C"
    integrity_and_resilience_a
}

# C with only Availability A → Scoped + EA
assessment_required if {
    input.criticality == "C"
    input.availability == "A"
    not security_a1
    not integrity_and_resilience_a
}
assessment_mandatory := false if {
    input.criticality == "C"
    input.availability == "A"
    not security_a1
    not integrity_and_resilience_a
}
arb_domains_set["EA"] if {
    input.criticality == "C"
    input.availability == "A"
    not security_a1
    not integrity_and_resilience_a
}
rule_messages["Criticality C + availability=A (no sec A1, no integrity+resilience=A) → Scoped review + EA"] if {
    input.criticality == "C"
    input.availability == "A"
    not security_a1
    not integrity_and_resilience_a
}

# C with all CIA+S ≤ B (no A-levels) → Express
assessment_required := false if {
    input.criticality == "C"
    c_all_leq_b
    not security_a1
    not integrity_and_resilience_a
    not availability_a
}
assessment_mandatory := false if {
    input.criticality == "C"
    c_all_leq_b
    not security_a1
    not integrity_and_resilience_a
    not availability_a
}
rule_messages["Criticality C + all CIA+S ≤ B (no A-levels) → Express to prod"] if {
    input.criticality == "C"
    c_all_leq_b
    not security_a1
    not integrity_and_resilience_a
    not availability_a
}

########################################
# Criticality D
########################################
# D with all D → Express
assessment_required := false if { input.criticality == "D"; all_d }
assessment_mandatory := false if { input.criticality == "D"; all_d }
rule_messages["Criticality D + all CIA+S=D → Express to prod"] if { input.criticality == "D"; all_d }

# D with any higher than D → Scoped
assessment_required if { input.criticality == "D"; not all_d }
assessment_mandatory := false if { input.criticality == "D"; not all_d }

arb_domains_set["Security"]           if { input.criticality == "D"; input.security == "A1" }
arb_domains_set["Security"]           if { input.criticality == "D"; input.security == "A2" }
arb_domains_set["Data"]               if { input.criticality == "D"; input.integrity == "A" }
arb_domains_set["EA"]                 if { input.criticality == "D"; input.availability == "A" }
arb_domains_set["Service Transition"] if { input.criticality == "D"; input.resilience == "A" }
arb_domains_set["EA"]                 if { input.criticality == "D"; not any_a_level; not all_d }

rule_messages["Criticality D + mix of D and ≤C → Scoped review + relevant domain(s)"] if {
    input.criticality == "D"
    not all_d
}

########################################
# Dependency Rule
########################################
# Always include EA; force at least Scoped
arb_domains_set["EA"] if { input.has_dependencies }
rule_messages["Application has dependencies → EA included"] if { input.has_dependencies }

########################################
# Review Mode (single priority decision)
########################################
# Full review conditions (highest priority)
requires_full_review if { input.criticality == "A" }
requires_full_review if { input.criticality == "B"; b_mandatory_trigger }
requires_full_review if { input.criticality == "C"; input.security == "A1" }
requires_full_review if { input.criticality == "C"; integrity_and_resilience_a }

# Scoped review conditions (second priority)
requires_scoped_review if {
    input.criticality == "B"
    not b_mandatory_trigger
    b_all_leq_b
}
requires_scoped_review if {
    input.criticality == "C"
    input.availability == "A"
    not security_a1
    not integrity_and_resilience_a
}
requires_scoped_review if { input.criticality == "D"; not all_d }
requires_scoped_review if { input.has_dependencies }

# Priority assignment: Full > Scoped > Express (default)
review_mode := "Full review" if { requires_full_review }
review_mode := "Scoped review" if { not requires_full_review; requires_scoped_review }

########################################
# Full review → include ALL ARB domains
########################################
arb_domains_set["EA"]                 if { review_mode == "Full review" }
arb_domains_set["Security"]           if { review_mode == "Full review" }
arb_domains_set["Data"]               if { review_mode == "Full review" }
arb_domains_set["Service Transition"] if { review_mode == "Full review" }

########################
# Helpers
########################
sec_not_a1a2 if { not input.security == "A1"; not input.security == "A2" }
security_a1  if { input.security == "A1" }
availability_a if { input.availability == "A" }
integrity_and_resilience_a if { input.integrity == "A"; input.resilience == "A" }

all_d if {
    input.security    == "D"
    input.integrity   == "D"
    input.availability == "D"
    input.resilience  == "D"
}

any_a_level if { input.security   == "A1" }
any_a_level if { input.security   == "A2" }
any_a_level if { input.integrity  == "A"  }
any_a_level if { input.availability == "A" }
any_a_level if { input.resilience == "A" }

is_leq_b(x) if {
    x in {"B", "C", "D"}
}

b_mandatory_trigger if { input.criticality == "B"; input.security   == "A1" }
b_mandatory_trigger if { input.criticality == "B"; input.security   == "A2" }
b_mandatory_trigger if { input.criticality == "B"; input.integrity  == "A"  }
b_mandatory_trigger if { input.criticality == "B"; input.availability == "A" }
b_mandatory_trigger if { input.criticality == "B"; input.resilience == "A" }

b_all_leq_b if { sec_not_a1a2; is_leq_b(input.integrity); is_leq_b(input.availability); is_leq_b(input.resilience) }
c_all_leq_b if { sec_not_a1a2; is_leq_b(input.integrity); is_leq_b(input.availability); is_leq_b(input.resilience) }

ordered_domains(set_in) = out if {
    desired := ["EA", "Security", "Data", "Service Transition"]
    out := [d | desired[_] == d; set_in[d]]
}

########################
# Final result object
########################
result := {
    "assessment_required":    assessment_required,
    "assessment_mandatory":   assessment_mandatory,
    "questionnaire_required": assessment_required,
    "attestation_required":   true,   # always required
    "arb_domains":            arb_domains,
    "review_mode":            review_mode,
    "fired_rules":            sort(fired_rules),
}
