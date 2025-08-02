\copy (
  SELECT 
    vs.epoch, 
    vs.jito_steward_overall_rank, 
    vs.activated_stake / 1000000000 AS activated_stake_sol, 
    vs.country, 
    vs.continent, 
    vs.delegator_compound_total_apy, 
    vi.name, 
    vs.identity_pubkey 
  FROM validator_stats vs 
  LEFT JOIN validator_info vi ON vs.identity_pubkey = vi.identity_pubkey 
  WHERE vs.epoch = 785 
  ORDER BY vs.jito_steward_overall_rank
) 
TO 'jota_data_epoch_785.csv' 
WITH (FORMAT CSV, HEADER);