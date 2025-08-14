package change_arb

default risk_assessment_required := "No"
default review_mode := "Express to prod"

arb_domains := ordered_domains(arb_domains_set)

########################
# Crown Jewels (A)
########################
risk_assessment_required := "Yes – Mandatory" if {
  input.criticality == "A"
}
review_mode := "Full review" if {
  input.criticality == "A"
}
arb_domains_set["EA"] if { input.criticality == "A" }
arb_domains_set["Security"] if { input.criticality == "A" }
arb_domains_set["Data"] if { input.criticality == "A" }
arb_domains_set["Service Transition"] if { input.criticality == "A" }

########################
# Criticality B
########################
risk_assessment_required := "Yes – Mandatory" if {
  input.criticality == "B"
  b_mandatory_trigger
}
review_mode := "Full review" if {
  input.criticality == "B"
  b_mandatory_trigger
}
arb_domains_set["Security"] if { input.criticality == "B"; input.security == "A1" }
arb_domains_set["Security"] if { input.criticality == "B"; input.security == "A2" }
arb_domains_set["Data"] if { input.criticality == "B"; input.integrity == "A" }
arb_domains_set["EA"] if { input.criticality == "B"; input.availability == "A" }
arb_domains_set["Service Transition"] if { input.criticality == "B"; input.resilience == "A" }

b_mandatory_trigger if {
  input.criticality == "B"
  input.security == "A1"
} or if {
  input.criticality == "B"
  input.security == "A2"
} or if {
  input.criticality == "B"
  input.integrity == "A"
} or if {
  input.criticality == "B"
  input.availability == "A"
} or if {
  input.criticality == "B"
  input.resilience == "A"
}

risk_assessment_required := "Yes – Conditional" if {
  input.criticality == "B"
  not b_mandatory_trigger
  b_all_leq_b
}
review_mode := "Scoped review" if {
  input.criticality == "B"
  not b_mandatory_trigger
  b_all_leq_b
}
arb_domains_set["EA"] if { input.criticality == "B"; not b_mandatory_trigger; b_all_leq_b }

b_all_leq_b if {
  sec_not_a1a2
  is_leq_b(input.integrity)
  is_leq_b(input.availability)
  is_leq_b(input.resilience)
}

########################
# Criticality C
########################
risk_assessment_required := "Yes – Mandatory" if {
  input.criticality == "C"
  input.security == "A1"
}
review_mode := "Full review" if {
  input.criticality == "C"
  input.security == "A1"
}
arb_domains_set["Security"] if { input.criticality == "C"; input.security == "A1" }

risk_assessment_required := "Yes – Mandatory" if {
  input.criticality == "C"
  integrity_and_resilience_a
}
review_mode := "Full review" if {
  input.criticality == "C"
  integrity_and_resilience_a
}
arb_domains_set["Data"] if { input.criticality == "C"; integrity_and_resilience_a }
arb_domains_set["Service Transition"] if { input.criticality == "C"; integrity_and_resilience_a }

risk_assessment_required := "Yes – Conditional" if {
  input.criticality == "C"
  input.availability == "A"
  not security_a1
  not integrity_and_resilience_a
}
review_mode := "Scoped review" if {
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

risk_assessment_required := "No" if {
  input.criticality == "C"
  c_all_leq_b
  not security_a1
  not integrity_and_resilience_a
  not availability_a
}
review_mode := "Express to prod" if {
  input.criticality == "C"
  c_all_leq_b
  not security_a1
  not integrity_and_resilience_a
  not availability_a
}
c_all_leq_b if {
  sec_not_a1a2
  is_leq_b(input.integrity)
  is_leq_b(input.availability)
  is_leq_b(input.resilience)
}

########################
# Criticality D
########################
risk_assessment_required := "No" if {
  input.criticality == "D"
  input.security == "D"
  input.integrity == "D"
  input.availability == "D"
  input.resilience == "D"
}
review_mode := "Express to prod" if {
  input.criticality == "D"
  input.security == "D"
  input.integrity == "D"
  input.availability == "D"
  input.resilience == "D"
}

risk_assessment_required := "Yes – Conditional" if {
  input.criticality == "D"
  not all_d
}
review_mode := "Scoped review" if {
  input.criticality == "D"
  not all_d
}

arb_domains_set["Security"] if { input.criticality == "D"; input.security == "A1" }
arb_domains_set["Security"] if { input.criticality == "D"; input.security == "A2" }
arb_domains_set["Data"] if { input.criticality == "D"; input.integrity == "A" }
arb_domains_set["EA"] if { input.criticality == "D"; input.availability == "A" }
arb_domains_set["Service Transition"] if { input.criticality == "D"; input.resilience == "A" }
arb_domains_set["EA"] if {
  input.criticality == "D"
  not any_a_level
  not all_d
}

########################
# Helpers
########################
sec_not_a1a2 if {
  not input.security == "A1"
  not input.security == "A2"
}

security_a1 if {
  input.security == "A1"
}

availability_a if {
  input.availability == "A"
}

integrity_and_resilience_a if {
  input.integrity == "A"
  input.resilience == "A"
}

all_d if {
  input.security == "D"
  input.integrity == "D"
  input.availability == "D"
  input.resilience == "D"
}

any_a_level if {
  input.security == "A1"
} or if {
  input.security == "A2"
} or if {
  input.integrity == "A"
} or if {
  input.availability == "A"
} or if {
  input.resilience == "A"
}

is_leq_b(x) if { x == "B" }
is_leq_b(x) if { x == "C" }
is_leq_b(x) if { x == "D" }

ordered_domains(set_in) := out if {
  desired := ["EA","Security","Data","Service Transition"]
  out := [d | d := desired[_]; set_in[d]]
}

########################
# Final result object
########################
result := {
  "risk_assessment_required": risk_assessment_required,
  "arb_domains": arb_domains,
  "review_mode": review_mode,
}
