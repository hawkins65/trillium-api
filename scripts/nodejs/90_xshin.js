/**
 * xshin stats fetcher
 * 
 * fetch and decode validator data from the shinobis perf pool.
 * based on the load.js and main.js from the xshin.fi website.
 */

const axios = require('axios');
const fs = require('fs').promises;
const { setupLogging } = require('./999_logging_config');

// Setup unified logging
const logger = setupLogging('90_xshin');

function base(ALPHABET) {
  if (ALPHABET.length >= 255) { throw new TypeError('Alphabet too long') }
  const BASE_MAP = new Uint8Array(256);
  for (let j = 0; j < BASE_MAP.length; j++) {
    BASE_MAP[j] = 255;
  }
  for (let i = 0; i < ALPHABET.length; i++) {
    const x = ALPHABET.charAt(i);
    const xc = x.charCodeAt(0);
    if (BASE_MAP[xc] !== 255) { throw new TypeError(x + ' is ambiguous') }
    BASE_MAP[xc] = i;
  }
  const BASE = ALPHABET.length;
  const LEADER = ALPHABET.charAt(0);
  const FACTOR = Math.log(BASE) / Math.log(256);
  const iFACTOR = Math.log(256) / Math.log(BASE);
  
  function encode(source) {
    if (source instanceof Uint8Array) {
    } else if (ArrayBuffer.isView(source)) {
      source = new Uint8Array(source.buffer, source.byteOffset, source.byteLength);
    } else if (Array.isArray(source)) {
      source = Uint8Array.from(source);
    }
    if (!(source instanceof Uint8Array)) { throw new TypeError('Expected Uint8Array') }
    if (source.length === 0) { return '' }
    
    let zeroes = 0;
    let length = 0;
    let pbegin = 0;
    const pend = source.length;
    while (pbegin !== pend && source[pbegin] === 0) {
      pbegin++;
      zeroes++;
    }
    
    const size = ((pend - pbegin) * iFACTOR + 1) >>> 0;
    const b58 = new Uint8Array(size);
    
    while (pbegin !== pend) {
      let carry = source[pbegin];
      let i = 0;
      for (let it1 = size - 1; (carry !== 0 || i < length) && (it1 !== -1); it1--, i++) {
        carry += (256 * b58[it1]) >>> 0;
        b58[it1] = (carry % BASE) >>> 0;
        carry = (carry / BASE) >>> 0;
      }
      if (carry !== 0) { throw new Error('Non-zero carry') }
      length = i;
      pbegin++;
    }
    
    let it2 = size - length;
    while (it2 !== size && b58[it2] === 0) {
      it2++;
    }
    
    let str = LEADER.repeat(zeroes);
    for (; it2 < size; ++it2) { str += ALPHABET.charAt(b58[it2]) }
    return str;
  }
  
  function decodeUnsafe(source) {
    if (typeof source !== 'string') { throw new TypeError('Expected String') }
    if (source.length === 0) { return new Uint8Array() }
    let psz = 0;
    
    let zeroes = 0;
    let length = 0;
    while (source[psz] === LEADER) {
      zeroes++;
      psz++;
    }
    
    const size = (((source.length - psz) * FACTOR) + 1) >>> 0;
    const b256 = new Uint8Array(size);
    
    while (source[psz]) {
      let carry = BASE_MAP[source.charCodeAt(psz)];
      if (carry === 255) { return }
      let i = 0;
      for (let it3 = size - 1; (carry !== 0 || i < length) && (it3 !== -1); it3--, i++) {
        carry += (BASE * b256[it3]) >>> 0;
        b256[it3] = (carry % 256) >>> 0;
        carry = (carry / 256) >>> 0;
      }
      if (carry !== 0) { throw new Error('Non-zero carry') }
      length = i;
      psz++;
    }
    
    let it4 = size - length;
    while (it4 !== size && b256[it4] === 0) {
      it4++;
    }
    const vch = new Uint8Array(zeroes + (size - it4));
    let j = zeroes;
    while (it4 !== size) {
      vch[j++] = b256[it4++];
    }
    return vch;
  }
  
  function decode(string) {
    const buffer = decodeUnsafe(string);
    if (buffer) { return buffer }
    throw new Error('Non-base' + BASE + ' character');
  }
  
  return {
    encode,
    decodeUnsafe,
    decode
  }
}

const bs58 = base('123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz');

/**
 * binary decoder functions
 */
function bincode_parser(buffer) {
  return {
    buffer: buffer,
    offset: 0
  };
}

function bincode_parser_advance(return_value, parser, count) {
  parser.offset += count;
  return return_value;
}

function bincode_parser_data_view(parser, length) {
  return new DataView(parser.buffer, parser.offset);
}

function bincode_parser_peek(parser) {
  return bincode_parser_data_view(parser, 1).getUint8();
}

function bincode_parser_slice(parser, length) {
  return parser.buffer.slice(parser.offset, parser.offset + length);
}

function bincode_bool(parser) {
  switch (bincode_parser_data_view(parser).getUint8()) {
    case 0:
      return bincode_parser_advance(false, parser, 1);
    case 1:
      return bincode_parser_advance(true, parser, 1);
    default:
      throw new Error("Failed to parse bool");
  }
}

function bincode_u8(parser) {
  return bincode_parser_advance(bincode_parser_data_view(parser).getUint8(), parser, 1);
}

function bincode_u16(parser) {
  switch (bincode_parser_peek(parser)) {
    case 251:
      return bincode_parser_advance(bincode_parser_data_view(parser).getUint16(1, true), parser, 3);
    default:
      return bincode_parser_advance(bincode_parser_data_view(parser).getUint8(0, true), parser, 1);
  }
}

function bincode_u32(parser) {
  switch (bincode_parser_peek(parser)) {
    case 251:
      return bincode_parser_advance(bincode_parser_data_view(parser).getUint16(1, true), parser, 3);
    case 252:
      return bincode_parser_advance(bincode_parser_data_view(parser).getUint32(1, true), parser, 5);
    default:
      return bincode_parser_advance(bincode_parser_data_view(parser).getUint8(0, true), parser, 1);
  }
}

function bincode_u64(parser) {
  switch (bincode_parser_peek(parser)) {
    case 251:
      return bincode_parser_advance(BigInt(bincode_parser_data_view(parser).getUint16(1, true)), parser, 3);
    case 252:
      return bincode_parser_advance(BigInt(bincode_parser_data_view(parser).getUint32(1, true)), parser, 5);
    case 253:
      return bincode_parser_advance(bincode_parser_data_view(parser).getBigUint64(1, true), parser, 9);
    default:
      return bincode_parser_advance(BigInt(bincode_parser_data_view(parser).getUint8(0, true)), parser, 1);
  }
}

function bincode_i64(parser) {
  let u64 = bincode_u64(parser);

  // Now un-zig-zag
  let one = BigInt(1);

  if ((u64 & BigInt(0x1)) == BigInt(0x1)) {
    return -((u64 + one) >> one);
  }
  else {
    return u64 >> one;
  }
}

function bincode_f32(parser) {
  return bincode_parser_advance(bincode_parser_data_view(parser).getFloat32(0, true), parser, 4);
}

function bincode_f64(parser) {
  return bincode_parser_advance(bincode_parser_data_view(parser).getFloat64(0, true), parser, 8);
}

function bincode_string(parser) {
  let length = bincode_u32(parser);
  return bincode_parser_advance((new TextDecoder()).decode(bincode_parser_slice(parser, length)), parser, length);
}

function bincode_pubkey(parser) {
  return bincode_parser_advance(bs58.encode(new Uint8Array(bincode_parser_slice(parser, 32))), parser, 32);
}

function bincode_vote_account_metrics(parser) {
  let leader_data = new Map();

  let count = bincode_u64(parser);

  for (let i = 0; i < count; i++) {
    leader_data.set(bincode_u64(parser), bincode_leader_data(parser));
  }

  let voter_data = new Map();

  count = bincode_u64(parser);

  for (let i = 0; i < count; i++) {
    voter_data.set(bincode_u64(parser), bincode_voter_data(parser));
  }

  let pool_data = new Map();

  count = bincode_u64(parser);

  for (let i = 0; i < count; i++) {
    pool_data.set(bincode_u64(parser), bincode_pool_data(parser));
  }

  return {
    leader_data: leader_data,
    voter_data: voter_data,
    pool_data: pool_data
  };
}

function bincode_leader_data(parser) {
  return {
    leader_slots: bincode_u64(parser),
    leader_groups: bincode_u64(parser),
    blocks: bincode_u64(parser),
    prior_skips: bincode_u64(parser),
    subsequent_skips: bincode_u64(parser),
    total_cu: bincode_u64(parser),
    total_vote_tx: bincode_u64(parser)
  };
}

function bincode_voter_data(parser) {
  let commission = bincode_u8(parser);
  let vote_credits = bincode_u64(parser);
  let total_fork_slots_voted_on = bincode_u64(parser);
  let total_fork_slot_vote_latency = bincode_u64(parser);
  let total_low_latency_fork_slots = bincode_u64(parser);
  let total_successful_vote_tx = bincode_u64(parser);
  let total_consensus_vote_tx = bincode_u64(parser);
  let delinquency_fraction = bincode_f64(parser);
  let apy = bincode_f32(parser);
  let count = bincode_u64(parser);
  let shared_identity_vote_accounts = [];

  for (let i = 0; i < count; i++) {
    shared_identity_vote_accounts.push(bincode_pubkey(parser));
  }

  let geo_concentration_city = null;
  let geo_concentration_country = null;

  if (bincode_bool(parser)) {
    geo_concentration_city = bincode_f64(parser);
    geo_concentration_country = bincode_f64(parser);
  }

  return {
    commission: commission,
    vote_credits: vote_credits,
    total_fork_slots_voted_on: total_fork_slots_voted_on,
    total_fork_slot_vote_latency: total_fork_slot_vote_latency,
    total_low_latency_fork_slots: total_low_latency_fork_slots,
    total_successful_vote_tx: total_successful_vote_tx,
    total_consensus_vote_tx: total_consensus_vote_tx,
    delinquency_fraction: delinquency_fraction,
    apy: apy,
    shared_identity_vote_accounts: shared_identity_vote_accounts,
    geo_concentration_city: geo_concentration_city,
    geo_concentration_country: geo_concentration_country
  };
}

function bincode_pool_data(parser) {
  return {
    extra_lamports: bincode_u64(parser),
    pool_lamports: bincode_u64(parser)
  };
}

function bincode_pool_voter_details(parser, version) {
  let details = bincode_voter_details(parser, version);
  let pool_stake = bincode_stake(parser);
  let count = bincode_u64(parser);
  let noneligibility_reasons = [];

  for (let i = 0; i < count; i++) {
    noneligibility_reasons.push(bincode_noneligibility_reason(parser));
  }

  return {
    details: details,
    pool_stake: pool_stake,
    noneligibility_reasons: noneligibility_reasons
  };
}

function bincode_non_pool_voter_details(parser, version) {
  let details = bincode_voter_details(parser, version);
  let count = bincode_u64(parser);
  let noneligibility_reasons = [];

  for (let i = 0; i < count; i++) {
    noneligibility_reasons.push(bincode_noneligibility_reason(parser));
  }

  return {
    details: details,
    noneligibility_reasons: noneligibility_reasons
  };
}

function bincode_voter_details(parser, version) {
  let name = null;
  if (bincode_bool(parser)) {
    name = bincode_string(parser);
  }

  let icon_url = null;
  if (bincode_bool(parser)) {
    icon_url = bincode_string(parser);
  }

  let details = null;
  if (bincode_bool(parser)) {
    details = bincode_string(parser);
  }

  let website_url = null;
  if (bincode_bool(parser)) {
    website_url = bincode_string(parser);
  }

  let city = null;
  if (bincode_bool(parser)) {
    city = bincode_string(parser);
  }

  let country = null;
  if (bincode_bool(parser)) {
    country = bincode_string(parser);
  }

  let stake = bincode_stake(parser);
  let target_pool_stake = bincode_u64(parser);
  let raw_score = bincode_score(parser, version);
  let normalized_score;

  if (version < 2) {
    normalized_score = {
      skip_rate: 0.0,
      prior_skip_rate: 0.0,
      subsequent_skip_rate: 0.0,
      cu: 0.0,
      latency: 0.0,
      llv: 0.0,
      cv: 0.0,
      vote_inclusion: 0.0,
      apy: 0.0,
      pool_extra_lamports: 0.0,
      city_concentration: 0.0,
      country_concentration: 0.0
    }
  }
  else {
    normalized_score = bincode_score(parser, version);
  }

  let total_score = bincode_f64(parser);

  return {
    name: name,
    icon_url: icon_url ? icon_url.replace('**icon_url**', '') : null,
    details: details,
    website_url: website_url,
    city: city,
    country: country,
    stake: stake,
    target_pool_stake: target_pool_stake,
    raw_score: raw_score,
    normalized_score: normalized_score,
    total_score: total_score
  };
}

function bincode_vote_inclusion(parser, version) {
  if (version < 1) {
    return 0;
  }
  else {
    return bincode_f64(parser);
  }
}

function bincode_score(parser, version) {
  return {
    skip_rate: bincode_f64(parser),
    prior_skip_rate: bincode_f64(parser),
    subsequent_skip_rate: bincode_f64(parser),
    cu: bincode_f64(parser),
    latency: bincode_f64(parser),
    llv: bincode_f64(parser),
    cv: bincode_f64(parser),
    vote_inclusion: bincode_vote_inclusion(parser, version),
    apy: bincode_f32(parser),
    pool_extra_lamports: bincode_f64(parser),
    city_concentration: bincode_f64(parser),
    country_concentration: bincode_f64(parser)
  };
}

function bincode_stake(parser) {
  return {
    active: bincode_u64(parser),
    activating: bincode_u64(parser),
    deactivating: bincode_u64(parser)
  };
}

function bincode_noneligibility_reason(parser) {
  let index = bincode_u64(parser);

  switch (Number(index)) {
    case 0:
      return "Blacklisted (" + bincode_string(parser) + ")";

    case 1:
      return "In superminority";

    case 2: {
      let s = "Not leader in recent epochs (";
      let comma = false;
      let count = bincode_u64(parser);
      for (let i = 0; i < count; i++) {
        if (comma) {
          s += ", ";
        }
        comma = true;
        s += bincode_u64(parser);
      }
      return s + ")";
    }

    case 3: {
      let s = "Low credits in recent epochs (";
      let comma = false;
      let count = bincode_u64(parser);
      for (let i = 0; i < count; i++) {
        if (comma) {
          s += ", ";
        }
        comma = true;
        s += bincode_u64(parser);
      }
      return s + ")";
    }

    case 4: {
      let s = "Excessive delinquency in recent epochs (";
      let comma = false;
      let count = bincode_u64(parser);
      for (let i = 0; i < count; i++) {
        if (comma) {
          s += ", ";
        }
        comma = true;
        s += bincode_u64(parser);
      }
      return s + ")";
    }

    case 5:
      return "Shared vote accounts";

    case 6:
      return "Commission too high: " + bincode_u8(parser);

    case 7: {
      let s = "APY too low in recent epochs (";
      let comma = false;
      let count = bincode_u64(parser);
      for (let i = 0; i < count; i++) {
        if (comma) {
          s += ", ";
        }
        comma = true;
        s += bincode_u64(parser);
      }
      return s + ")";
    }

    case 8:
      return "Insufficient branding";

    case 9:
      return "Insufficient non-pool stake";

    default:
      throw "Invalid non eligibility reason: " + index;
  }
}

function bincode_search(parser) {
  let version = bincode_u8(parser);
  if (version != 0) {
    throw new Error("Unexpected version: " + version);
  }

  let count = bincode_u64(parser);

  let voters = new Map();

  for (let i = 0; i < count; i++) {
    let pubkey = bincode_pubkey(parser);

    if (bincode_bool(parser)) {
      voters.set(pubkey, bincode_string(parser));
    }
    else {
      voters.set(pubkey, null);
    }
  }

  return {
    voters: voters
  };
}

function bincode_voter_metrics_group(parser) {
  let version = bincode_u8(parser);
  if (version != 0) {
    throw new Error("Unexpected version: " + version);
  }

  let count = bincode_u64(parser);

  let voters = new Map();

  for (let i = 0; i < count; i++) {
    voters.set(bincode_pubkey(parser), bincode_vote_account_metrics(parser));
  }

  return {
    voters: voters
  };
}

function bincode_non_pool_voters(parser) {
  let version = bincode_u8(parser);
  if ((version < 0) || (version > 2)) {
    throw new Error("Unexpected version: " + version);
  }

  let count = bincode_u64(parser);

  let voters = new Map();

  for (let i = 0; i < count; i++) {
    voters.set(bincode_pubkey(parser), bincode_non_pool_voter_details(parser, version));
  }

  return {
    voters: voters
  };
}

function bincode_overview_details(parser) {
  let version = bincode_u8(parser);
  if (version != 0) {
    throw new Error("Unexpected version: " + version);
  }

  return {
    price: bincode_f32(parser),
    epoch: bincode_u64(parser),
    epoch_start: bincode_u64(parser),
    epoch_duration: bincode_u64(parser),
    pool_stake: bincode_stake(parser),
    reserve: bincode_u64(parser),
    apy: bincode_f32(parser)
  };
}

function bincode_pool_details(parser) {
  let version = bincode_u8(parser);
  if ((version < 0) || (version > 2)) {
    throw new Error("Unexpected version: " + version);
  }

  let pool_validator_count = bincode_u64(parser);

  let best_skip_rate = bincode_best_f64(parser);
  let best_cu = bincode_best_f64(parser);
  let best_latency = bincode_best_f64(parser);
  let best_llv = bincode_best_f64(parser);
  let best_cv = bincode_best_f64(parser);

  let best_vote_inclusion;
  if (version < 1) {
    best_vote_inclusion = [];
  }
  else {
    best_vote_inclusion = bincode_best_f64(parser);
  }

  let best_apy = bincode_best_f32(parser);
  let best_pool_extra_lamports = bincode_best_f64(parser);
  let best_city_concentration = bincode_best_f64(parser);
  let best_country_concentration = bincode_best_f64(parser);
  let best_overall = bincode_best_f64(parser);

  let compare_by_current = bincode_comparative_metrics(parser, version);
  let compare_by_target = bincode_comparative_metrics(parser, version);

  let count = bincode_u64(parser);
  let pool_voters = new Map();

  for (let i = 0; i < count; i++) {
    pool_voters.set(bincode_pubkey(parser), bincode_pool_voter_details(parser, version));
  }

  let inclusion_weights;
  let ranking_weights;

  if (version < 1) {
    inclusion_weights = {
      skip_rate_weight: 0,
      prior_skip_rate_weight: 0,
      subsequent_skip_rate_weight: 0,
      cu_weight: 0,
      latency_weight: 0,
      llv_weight: 0,
      cv_weight: 0,
      vote_inclusion_weight: 0,
      apy_weight: 0,
      city_concentration_weight: 0,
      country_concentration_weight: 0
    };
    ranking_weights = {
      skip_rate_weight: 0,
      prior_skip_rate_weight: 0,
      subsequent_skip_rate_weight: 0,
      cu_weight: 0,
      latency_weight: 0,
      llv_weight: 0,
      cv_weight: 0,
      vote_inclusion_weight: 0,
      apy_weight: 0,
      city_concentration_weight: 0,
      country_concentration_weight: 0
    };
  }
  else {
    inclusion_weights = bincode_metric_weights(parser);
    ranking_weights = bincode_metric_weights(parser);
  }

  return {
    pool_validator_count: pool_validator_count,
    best_skip_rate: best_skip_rate,
    best_cu: best_cu,
    best_latency: best_latency,
    best_llv: best_llv,
    best_cv: best_cv,
    best_vote_inclusion: best_vote_inclusion,
    best_apy: best_apy,
    best_pool_extra_lamports: best_pool_extra_lamports,
    best_city_concentration: best_city_concentration,
    best_country_concentration: best_country_concentration,
    best_overall: best_overall,
    compare_by_current: compare_by_current,
    compare_by_target: compare_by_target,
    pool_voters: pool_voters,
    inclusion_weights: inclusion_weights,
    ranking_weights: ranking_weights
  };
}

function bincode_best_f64(parser) {
  let count = bincode_u64(parser);
  let best = [];

  for (let i = 0; i < count; i++) {
    best.push({
      pubkey: bincode_pubkey(parser),
      metric: bincode_f64(parser),
      ranking: bincode_u64(parser)
    });
  }

  return best;
}

function bincode_best_f32(parser) {
  let count = bincode_u64(parser);
  let best = [];

  for (let i = 0; i < count; i++) {
    best.push({
      pubkey: bincode_pubkey(parser),
      metric: bincode_f32(parser),
      ranking: bincode_u64(parser)
    });
  }

  return best;
}

function bincode_comparative_metrics(parser, version) {
  return {
    spp_raw: bincode_score(parser, version),
    other_raw: bincode_score(parser, version)
  };
}

function bincode_metric_weights(parser) {
  return {
    skip_rate_weight: bincode_f64(parser),
    prior_skip_rate_weight: bincode_f64(parser),
    subsequent_skip_rate_weight: bincode_f64(parser),
    cu_weight: bincode_f64(parser),
    latency_weight: bincode_f64(parser),
    llv_weight: bincode_f64(parser),
    cv_weight: bincode_f64(parser),
    vote_inclusion_weight: bincode_f64(parser),
    apy_weight: bincode_f64(parser),
    city_concentration_weight: bincode_f64(parser),
    country_concentration_weight: bincode_f64(parser)
  }
}

/**
 * api
 */
async function fetchLatestTimestamp() {
  try {
    const response = await axios.get('https://xshin.fi/data/pool/newest');
    return Number(response.data);
  } catch (error) {
    logger.error('❌ Error fetching latest timestamp:', error);
    throw error;
  }
}

async function fetchBinaryData(url) {
  try {
    const response = await axios.get(url, {
      responseType: 'arraybuffer'
    });
    return response.data;
  } catch (error) {
    logger.error(`❌ Error fetching data from ${url}:`, error);
    throw error;
  }
}

async function loadSearch(timestamp) {
  try {
    const searchBin = await fetchBinaryData(`https://xshin.fi/data/pool/${timestamp}/search.bin`);
    const buffer = new Uint8Array(searchBin).buffer;
    return bincode_search(bincode_parser(buffer));
  } catch (error) {
    logger.error('❌ Error loading search data:', error);
    throw error;
  }
}

async function loadPoolDetails(timestamp) {
  try {
    const poolBin = await fetchBinaryData(`https://xshin.fi/data/pool/${timestamp}/pool.bin`);
    const buffer = new Uint8Array(poolBin).buffer;
    return bincode_pool_details(bincode_parser(buffer));
  } catch (error) {
    logger.error('❌ Error loading pool details:', error);
    throw error;
  }
}

async function loadNonPoolVoters(timestamp) {
  try {
    const nonPoolVotersBin = await fetchBinaryData(`https://xshin.fi/data/pool/${timestamp}/non_pool_voters.bin`);
    const buffer = new Uint8Array(nonPoolVotersBin).buffer;
    return bincode_non_pool_voters(bincode_parser(buffer));
  } catch (error) {
    logger.error('❌ Error loading non-pool voters:', error);
    throw error;
  }
}

async function loadOverviewDetails(timestamp) {
  try {
    const overviewBin = await fetchBinaryData(`https://xshin.fi/data/pool/${timestamp}/overview.bin`);
    const buffer = new Uint8Array(overviewBin).buffer;
    return bincode_overview_details(bincode_parser(buffer));
  } catch (error) {
    console.error('error loading overview details:', error);
    throw error;
  }
}

async function loadVoterMetricsGroup(timestamp, votePubkey) {
  try {
    const key = votePubkey.slice(-1);
    const groupBin = await fetchBinaryData(`https://xshin.fi/data/pool/${timestamp}/groups/${key}.bin`);
    const buffer = new Uint8Array(groupBin).buffer;
    return bincode_voter_metrics_group(bincode_parser(buffer));
  } catch (error) {
    console.error('error loading voter metrics group:', error);
    throw error;
  }
}

async function getValidatorByPubkey(pubkey, includeMetrics = true) {
  try {
    const timestamp = await fetchLatestTimestamp();
    
    const poolDetails = await loadPoolDetails(timestamp);
    let validator = poolDetails.pool_voters.get(pubkey);
    
    if (!validator) {
      const nonPoolVoters = await loadNonPoolVoters(timestamp);
      validator = nonPoolVoters.voters.get(pubkey);
      
      if (!validator) {
        throw new Error(`validator with pubkey ${pubkey} not found`);
      }
    }
    
    if (includeMetrics) {
      const metricsGroup = await loadVoterMetricsGroup(timestamp, pubkey);
      const metrics = metricsGroup.voters.get(pubkey);
      
      if (metrics) {
        validator.metrics = metrics;
      }
    }
    
    const search = await loadSearch(timestamp);
    const name = search.voters.get(pubkey);
    
    if (name) {
      validator.search_name = name;
    }
    
    return {
      timestamp,
      pubkey,
      validator
    };
  } catch (error) {
    console.error(`error fetching validator data for ${pubkey}:`, error);
    throw error;
  }
}

async function getValidatorsByAward(awardCategory) {
  try {
    const timestamp = await fetchLatestTimestamp();
    const poolDetails = await loadPoolDetails(timestamp);
    
    if (awardCategory && !poolDetails[awardCategory]) {
      throw new Error(`award category ${awardCategory} not found`);
    }
    
    const awardCategories = [
      'best_skip_rate',
      'best_cu',
      'best_latency',
      'best_llv',
      'best_cv',
      'best_vote_inclusion',
      'best_apy',
      'best_pool_extra_lamports',
      'best_city_concentration',
      'best_country_concentration',
      'best_overall'
    ];
    
    const result = awardCategory ? {} : [];
    
    const categoriesToProcess = awardCategory ? [awardCategory] : awardCategories;
    
    for (const category of categoriesToProcess) {
      const awardWinners = poolDetails[category];
      const categoryResult = [];
      
      for (const winner of awardWinners) {
        const validator = poolDetails.pool_voters.get(winner.pubkey);
        if (validator) {
          categoryResult.push({
            pubkey: winner.pubkey,
            metric: winner.metric,
            ranking: winner.ranking,
            validator: validator
          });
        }
      }
      
      if (awardCategory) {
        return categoryResult;
      } else {
        result.push({ category, winners: categoryResult });
      }
    }
    
    return result;
  } catch (error) {
    console.error(`error fetching validators for award ${awardCategory || 'all'}:`, error);
    throw error;
  }
}

async function getTopValidators(limit = 10) {
  try {
    const timestamp = await fetchLatestTimestamp();
    const poolDetails = await loadPoolDetails(timestamp);
    
    const validators = Array.from(poolDetails.pool_voters.entries())
      .map(([pubkey, validator]) => ({
        pubkey,
        total_score: validator.details.total_score,
        validator
      }))
      .sort((a, b) => b.total_score - a.total_score)
      .slice(0, limit);
    
    return validators;
  } catch (error) {
    console.error(`error fetching top validators:`, error);
    throw error;
  }
}

async function getAllValidators(type = 'pool') {
  try {
    const timestamp = await fetchLatestTimestamp();
    const poolDetails = await loadPoolDetails(timestamp);
    
    let validators;
    
    if (type === 'pool') {
      validators = Array.from(poolDetails.pool_voters.entries())
        .map(([pubkey, validator]) => ({
          pubkey,
          total_score: validator.details.total_score,
          validator
        }))
        .sort((a, b) => b.total_score - a.total_score);
    } else {
      const nonPoolVoters = await loadNonPoolVoters(timestamp);
      const allValidators = new Map([...poolDetails.pool_voters]);
      
      for (const [pubkey, validator] of nonPoolVoters.voters) {
        if (!allValidators.has(pubkey)) {
          allValidators.set(pubkey, {
            ...validator,
            pool_stake: { active: 0, activating: 0, deactivating: 0 }
          });
        }
      }
      
      validators = Array.from(allValidators.entries())
        .map(([pubkey, validator]) => ({
          pubkey,
          total_score: validator.details?.total_score || 0,
          validator
        }))
        .sort((a, b) => b.total_score - a.total_score);
    }
    
    return validators;
  } catch (error) {
    console.error(`error fetching all validators:`, error);
    throw error;
  }
}

async function saveToFile(data, filename) {
  try {
    await fs.writeFile(filename, JSON.stringify(data, (key, value) => {
      if (typeof value === 'bigint') {
        return value.toString();
      }
      if (value instanceof Map) {
        return Object.fromEntries(value);
      }
      return value;
    }, 2));
    console.log(`data saved to ${filename}`);
  } catch (error) {
    console.error(`error saving data to ${filename}:`, error);
    throw error;
  }
}

async function main() {
  try {
    if (process.argv[2] === 'validator' && process.argv[3]) {
      const pubkey = process.argv[3];
      const validator = await getValidatorByPubkey(pubkey);
      await saveToFile(validator, `validator_${pubkey.substring(0, 8)}.json`);
    } 
    else if (process.argv[2] === 'top') {
      const limit = process.argv[3] ? parseInt(process.argv[3]) : 10;
      const topValidators = await getTopValidators(limit);
      await saveToFile(topValidators, 'top_validators.json');
    }
    else if (process.argv[2] === 'award') {
      if (process.argv[3]) {
        const awardCategory = process.argv[3];
        const awardWinners = await getValidatorsByAward(awardCategory);
        await saveToFile(awardWinners, `${awardCategory}_winners.json`);
      } else {
        const allAwardWinners = await getValidatorsByAward();
        await saveToFile(allAwardWinners, 'all_award_winners.json');
      }
    }
    else if (process.argv[2] === 'all') {
      const type = process.argv[3] || 'pool';
      const allValidators = await getAllValidators(type);
      await saveToFile(allValidators, `all_${type}_validators.json`);
    }
    else if (process.argv[2] === 'overview') {
      const timestamp = await fetchLatestTimestamp();
      const overview = await loadOverviewDetails(timestamp);
      await saveToFile(overview, 'overview.json');
    }
    else {
      console.log(`
usage:
  node xshin.js validator <pubkey>         - get data for a specific validator (pubkey required)
  node xshin.js top [limit=10]             - get top validators
  node xshin.js award [<category>]         - get award winners for a category or all categories if none specified
  node xshin.js all [type=pool|all]        - get all validators (pool-only or all)
  node xshin.js overview                   - get overview data
      `);
    }
  } catch (error) {
    console.error('error in main function:', error);
  }
}

module.exports = {
  fetchLatestTimestamp,
  loadSearch,
  loadPoolDetails,
  loadNonPoolVoters,
  loadOverviewDetails,
  loadVoterMetricsGroup,
  getValidatorByPubkey,
  getValidatorsByAward,
  getTopValidators,
  getAllValidators,
  saveToFile
};

if (require.main === module) {
  main().catch(console.error);
}