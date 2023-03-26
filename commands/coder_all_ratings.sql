select c.coder_id
     , u.handle
     , NVL(r.rating,0) as algorithm_rating
     , NVL(dsr.rating,0) as design_rating
     , NVL(dvr.rating,0) as development_rating
     , NVL(cor.rating,0) as conceptualization_rating
     , NVL(spr.rating,0) as specification_rating
     , NVL(arr.rating,0) as architecture_rating
     , NVL(asr.rating,0) as assembly_rating
     , NVL(atr.rating,0) as test_suites_rating
     , NVL(ats.rating,0) as test_scenarios_rating
     , NVL(aui.rating,0) as ui_prototype_rating
     , NVL(arb.rating,0) as ria_build_rating
     , NVL(ar.rating,0) as hs_algorithm_rating
     , NVL(armm.rating,0) as marathon_match_rating
     , NVL(ccr.rating,0) as content_creation_rating
     , NVL(rep.rating,0) as reporting_rating
  from OUTER coder c
     , user u
     , OUTER(rating r)
     , OUTER(algo_rating ar)
     , OUTER(algo_rating armm)
     , OUTER(tcs_catalog:user_rating dsr)
     , OUTER(tcs_catalog:user_rating dvr)
     , OUTER(tcs_catalog:user_rating cor)
     , OUTER(tcs_catalog:user_rating spr)
     , OUTER(tcs_catalog:user_rating arr)
     , OUTER(tcs_catalog:user_rating asr)
     , OUTER(tcs_catalog:user_rating atr)
     , OUTER(tcs_catalog:user_rating ats)
     , OUTER(tcs_catalog:user_rating aui)
     , OUTER(tcs_catalog:user_rating arb)
     , OUTER(tcs_catalog:user_rating ccr)
     , OUTER(tcs_catalog:user_rating rep)
 where u.user_id = @cr@
   and u.user_id = c.coder_id
   and r.coder_id = u.user_id
   and dsr.user_id = u.user_id
   and dsr.phase_id = 112
   and dvr.user_id = u.user_id
   and dvr.phase_id = 113
   and cor.user_id = u.user_id
   and cor.phase_id = 134
   and spr.user_id = u.user_id
   and spr.phase_id = 117
   and arr.user_id = u.user_id
   and arr.phase_id = 118
   and asr.user_id = u.user_id
   and asr.phase_id = 125
   and atr.user_id = u.user_id
   and atr.phase_id = 124
   and ats.user_id = u.user_id
   and ats.phase_id = 137
   and aui.user_id = u.user_id
   and aui.phase_id = 130
   and arb.user_id = u.user_id
   and arb.phase_id = 135
   and ccr.user_id = u.user_id
   and ccr.phase_id = 146
   and rep.user_id = u.user_id
   and rep.phase_id = 147
   and ar.coder_id = u.user_id
   and ar.algo_rating_type_id = 2
   and armm.coder_id = u.user_id
   and armm.algo_rating_type_id = 3