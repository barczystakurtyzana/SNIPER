use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_response::RpcTokenAccountBalance,
    client_error::ClientError,
};
use solana_sdk::{
    pubkey::Pubkey,
    clock::Slot,
    commitment_config::{CommitmentConfig, CommitmentLevel},
};
use spl_token::state::Mint;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, RwLock, Semaphore, Mutex},
    task, time,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use anyhow::{anyhow, Result, Context};
use reqwest::Client;
use log::{info, warn, error, debug};
use std::cmp::{min, max};
use std::collections::BTreeMap;
use std::str::FromStr;
use governor::{DefaultDirectRateLimiter, Quota, RateLimiter};
use nonempty::NonEmpty;
use std::num::NonZeroU32;
use tokio_retry::{
    Retry,
    strategy::{ExponentialBackoff, jitter},
};

// Import types from crate
use crate::types::{PremintCandidate, QuantumCandidateGui};

// 1. Struktury danych
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredCandidate {
    pub base: PremintCandidate,
    pub predicted_score: u8,
    pub reason: String,
    pub feature_scores: HashMap<String, f64>,
    pub calculation_time: u128,
    pub anomaly_detected: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OracleConfig {
    pub weights: FeatureWeights,
    pub rpc_endpoints: NonEmpty<String>,
    pub pump_fun_api_key: Option<String>,
    pub bitquery_api_key: Option<String>,
    pub thresholds: ScoreThresholds,
    pub rpc_retry_attempts: usize,
    pub rpc_timeout_seconds: u64,
    pub cache_ttl_seconds: u64,
    pub max_parallel_requests: usize,
    pub rate_limit_requests_per_second: u32,
    pub notify_threshold: u8, // GUI notification threshold (default 75)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureWeights {
    pub liquidity: f64,
    pub holder_distribution: f64,
    pub volume_growth: f64,
    pub holder_growth: f64,
    pub price_change: f64,
    pub jito_bundle_presence: f64,
    pub creator_sell_speed: f64,
    pub metadata_quality: f64,
    pub social_activity: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreThresholds {
    pub min_liquidity_sol: f64,
    pub whale_threshold: f64,
    pub volume_growth_threshold: f64,
    pub holder_growth_threshold: f64,
    pub min_metadata_quality: f64,
    pub creator_sell_penalty_threshold: u64,
    pub social_activity_threshold: f64,
}

// 2. Główny moduł Oracle
pub struct PredictiveOracle {
    pub candidate_receiver: mpsc::Receiver<PremintCandidate>,
    pub scored_sender: mpsc::Sender<ScoredCandidate>,
    pub gui_suggestions: Arc<Mutex<Option<mpsc::Sender<QuantumCandidateGui>>>>,
    pub rpc_clients: NonEmpty<Arc<RpcClient>>,
    pub http_client: Client,
    pub config: OracleConfig,
    pub token_cache: RwLock<HashMap<Pubkey, (Instant, TokenData)>>,
    pub metrics: Arc<RwLock<OracleMetrics>>,
    pub rate_limiter: Arc<DefaultDirectRateLimiter>,
    pub request_semaphore: Arc<Semaphore>,
}

#[derive(Debug, Default)]
pub struct OracleMetrics {
    pub total_scored: u64,
    pub avg_scoring_time: f64,
    pub high_score_count: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub rpc_errors: u64,
    pub api_errors: u64,
}

#[derive(Debug, Clone)]
pub struct TokenData {
    pub supply: u64,
    pub decimals: u8,
    pub metadata_uri: String,
    pub metadata: Option<Metadata>,
    pub holder_distribution: Vec<HolderData>,
    pub liquidity_pool: Option<LiquidityPool>,
    pub volume_data: VolumeData,
    pub creator_holdings: CreatorHoldings,
    pub holder_history: VecDeque<usize>,
    pub price_history: VecDeque<f64>,
    pub social_activity: SocialActivity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub name: String,
    pub symbol: String,
    pub description: String,
    pub image: String,
    pub attributes: Vec<Attribute>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Attribute {
    pub trait_type: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct HolderData {
    pub address: Pubkey,
    pub percentage: f64,
    pub is_whale: bool,
}

#[derive(Debug, Clone)]
pub struct LiquidityPool {
    pub sol_amount: f64,
    pub token_amount: f64,
    pub pool_address: Pubkey,
    pub pool_type: PoolType,
}

#[derive(Debug, Clone)]
pub enum PoolType {
    Raydium,
    Orca,
    PumpFun,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct VolumeData {
    pub initial_volume: f64,
    pub current_volume: f64,
    pub volume_growth_rate: f64,
    pub transaction_count: u32,
    pub buy_sell_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct CreatorHoldings {
    pub initial_balance: u64,
    pub current_balance: u64,
    pub first_sell_timestamp: Option<u64>,
    pub sell_transactions: u32,
}

#[derive(Debug, Clone)]
pub struct SocialActivity {
    pub twitter_mentions: u32,
    pub telegram_members: u32,
    pub discord_members: u32,
    pub social_score: f64,
}

// 3. Implementacja Oracle
impl PredictiveOracle {
    pub fn new(
        candidate_receiver: mpsc::Receiver<PremintCandidate>,
        scored_sender: mpsc::Sender<ScoredCandidate>,
        config: OracleConfig,
    ) -> Result<Self> {
        let rpc_clients = config.rpc_endpoints
            .map(|endpoint| {
                let client = RpcClient::new_with_timeout(
                    endpoint,
                    Duration::from_secs(config.rpc_timeout_seconds)
                );
                Arc::new(client)
            });
        
        let quota = Quota::per_second(NonZeroU32::new(config.rate_limit_requests_per_second)
            .unwrap_or(NonZeroU32::new(10).unwrap()));
        let rate_limiter = Arc::new(RateLimiter::direct(quota));
        
        let request_semaphore = Arc::new(Semaphore::new(config.max_parallel_requests));

        Ok(Self {
            candidate_receiver,
            scored_sender,
            gui_suggestions: Arc::new(Mutex::new(None)),
            rpc_clients,
            http_client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()?,
            config,
            token_cache: RwLock::new(HashMap::new()),
            metrics: Arc::new(RwLock::new(OracleMetrics::default())),
            rate_limiter,
            request_semaphore,
        })
    }

    pub fn set_gui_sender(&self, sender: mpsc::Sender<QuantumCandidateGui>) {
        tokio::spawn({
            let gui_suggestions = self.gui_suggestions.clone();
            async move {
                let mut gui_lock = gui_suggestions.lock().await;
                *gui_lock = Some(sender);
            }
        });
    }

    pub async fn run(&mut self) {
        info!("Starting Predictive Oracle with {} RPC endpoints", self.rpc_clients.len());
        
        while let Some(candidate) = self.candidate_receiver.recv().await {
            let permit = self.request_semaphore.clone().acquire_owned().await;
            
            let oracle = self.clone();
            tokio::spawn(async move {
                let start_time = Instant::now();
                
                match oracle.score_candidate(&candidate).await {
                    Ok(mut scored) => {
                        let scoring_time = start_time.elapsed().as_micros();
                        scored.calculation_time = scoring_time;
                        
                        // Aktualizuj metryki
                        let mut metrics = oracle.metrics.write().await;
                        metrics.total_scored += 1;
                        metrics.avg_scoring_time = 
                            (metrics.avg_scoring_time * (metrics.total_scored - 1) as f64 
                             + scoring_time as f64) / metrics.total_scored as f64;
                        
                        if scored.predicted_score >= 80 {
                            metrics.high_score_count += 1;
                        }
                        drop(metrics);
                        
                        // Send GUI suggestion if score meets threshold
                        if scored.predicted_score >= oracle.config.notify_threshold {
                            let gui_suggestion = QuantumCandidateGui {
                                mint: candidate.mint,
                                score: scored.predicted_score,
                                reason: scored.reason.clone(),
                                feature_scores: scored.feature_scores.clone(),
                                timestamp: candidate.timestamp,
                            };
                            
                            if let Some(sender) = oracle.gui_suggestions.lock().await.as_ref() {
                                if let Err(e) = sender.send(gui_suggestion).await {
                                    warn!("Failed to send GUI suggestion: {}", e);
                                }
                            }
                        }
                        
                        // Wyślij wynik
                        if let Err(e) = oracle.scored_sender.send(scored.clone()).await {
                            error!("Failed to send scored candidate: {}", e);
                        }
                        
                        info!("Scored candidate: {} in {}μs. Score: {}",
                            candidate.mint, scoring_time, scored.predicted_score);
                    }
                    Err(e) => {
                        warn!("Failed to score candidate {}: {}", candidate.mint, e);
                    }
                }
                
                drop(permit);
            });
        }
    }

    async fn score_candidate(&self, candidate: &PremintCandidate) -> Result<ScoredCandidate> {
        // Pobierz dane tokena z retries
        let token_data = self.fetch_token_data_with_retries(candidate).await?;
        
        // Wykrywanie anomalii
        let anomaly_detected = self.detect_anomalies(&token_data);
        
        // Oblicz cechy
        let mut feature_scores = HashMap::new();
        
        // 1. Płynność
        let liquidity_score = self.calculate_liquidity_score(&token_data);
        feature_scores.insert("liquidity".to_string(), liquidity_score);
        
        // 2. Rozkład holderów
        let holder_score = self.calculate_holder_distribution_score(&token_data);
        feature_scores.insert("holder_distribution".to_string(), holder_score);
        
        // 3. Tempo wzrostu wolumenu
        let volume_score = self.calculate_volume_growth_score(&token_data);
        feature_scores.insert("volume_growth".to_string(), volume_score);
        
        // 4. Tempo przyrostu holderów
        let holder_growth_score = self.calculate_holder_growth_score(&token_data);
        feature_scores.insert("holder_growth".to_string(), holder_growth_score);
        
        // 5. Zmiana ceny
        let price_change_score = self.calculate_price_change_score(&token_data);
        feature_scores.insert("price_change".to_string(), price_change_score);
        
        // 6. Obecność w bundle Jito
        let jito_score = if candidate.is_jito_bundle.unwrap_or(false) { 1.0 } else { 0.0 };
        feature_scores.insert("jito_bundle_presence".to_string(), jito_score);
        
        // 7. Czas sprzedaży twórcy
        let creator_sell_score = self.calculate_creator_sell_score(&token_data, candidate.timestamp);
        feature_scores.insert("creator_sell_speed".to_string(), creator_sell_score);
        
        // 8. Jakość metadanych
        let metadata_score = self.calculate_metadata_score(&token_data).await;
        feature_scores.insert("metadata_quality".to_string(), metadata_score);
        
        // 9. Aktywność społeczności
        let social_score = self.calculate_social_score(&token_data);
        feature_scores.insert("social_activity".to_string(), social_score);
        
        // Oblicz wynik końcowy
        let predicted_score = self.calculate_predicted_score(&feature_scores);
        let reason = self.generate_reason(&feature_scores, predicted_score, anomaly_detected);
        
        Ok(ScoredCandidate {
            base: candidate.clone(),
            predicted_score,
            reason,
            feature_scores,
            calculation_time: 0,
            anomaly_detected,
        })
    }

    async fn fetch_token_data_with_retries(&self, candidate: &PremintCandidate) -> Result<TokenData> {
        let retry_strategy = ExponentialBackoff::from_millis(100)
            .max_delay(Duration::from_secs(5))
            .map(jitter)
            .take(self.config.rpc_retry_attempts);
        
        Retry::spawn(retry_strategy, || {
            self.fetch_token_data(candidate)
        }).await
    }

    async fn fetch_token_data(&self, candidate: &PremintCandidate) -> Result<TokenData> {
        // Sprawdź cache (read lock)
        {
            let cache = self.token_cache.read().await;
            if let Some((instant, data)) = cache.get(&candidate.mint) {
                if instant.elapsed().as_secs() < self.config.cache_ttl_seconds {
                    let mut metrics = self.metrics.write().await;
                    metrics.cache_hits += 1;
                    return Ok(data.clone());
                }
            }
        }
        
        let mut metrics = self.metrics.write().await;
        metrics.cache_misses += 1;
        drop(metrics);
        
        // Rate limiting
        self.rate_limiter.until_ready().await;
        
        // Wybierz random RPC client dla load balancing
        let rpc_index = rand::thread_rng().gen_range(0..self.rpc_clients.len());
        let rpc_client = &self.rpc_clients[rpc_index];

        // Pobierz dane równolegle z lepszą obsługą błędów
        let metadata_fut = self.fetch_token_metadata(candidate, rpc_client);
        let holders_fut = self.fetch_holder_distribution(candidate, rpc_client);
        let liquidity_fut = self.fetch_liquidity_data(candidate, rpc_client);
        let volume_fut = self.fetch_volume_data(candidate, rpc_client);
        let creator_fut = self.fetch_creator_holdings(candidate, rpc_client);
        let offchain_fut = self.fetch_offchain_data(candidate);
        let social_fut = self.fetch_social_data(candidate);

        let (metadata_res, holders_res, liquidity_res, volume_res, creator_res, offchain_res, social_res) = tokio::join!(
            metadata_fut, holders_fut, liquidity_fut, volume_fut, creator_fut, offchain_fut, social_fut
        );

        let (supply, decimals, metadata_uri, metadata) = metadata_res?;
        let holder_distribution = holders_res?;
        let liquidity_pool = liquidity_res?;
        let volume_data = volume_res?;
        let creator_holdings = creator_res?;
        let _ = offchain_res;
        let social_activity = social_res.unwrap_or_else(|_| SocialActivity {
            twitter_mentions: 0,
            telegram_members: 0,
            discord_members: 0,
            social_score: 0.0,
        });

        // Symuluj historię dla holderów i cen
        let mut holder_history = VecDeque::new();
        holder_history.push_back(holder_distribution.len());
        
        let mut price_history = VecDeque::new();
        if let Some(pool) = &liquidity_pool {
            let price = pool.sol_amount / (pool.token_amount / 10f64.powf(decimals as f64));
            price_history.push_back(price);
        }

        let token_data = TokenData {
            supply,
            decimals,
            metadata_uri,
            metadata,
            holder_distribution,
            liquidity_pool,
            volume_data,
            creator_holdings,
            holder_history,
            price_history,
            social_activity,
        };
        
        // Zapisz w cache (write lock)
        {
            let mut cache = self.token_cache.write().await;
            cache.insert(candidate.mint, (Instant::now(), token_data.clone()));
        }
        
        Ok(token_data)
    }

    async fn fetch_token_metadata(&self, candidate: &PremintCandidate, rpc: &RpcClient) -> Result<(u64, u8, String, Option<Metadata>)> {
        let account = rpc.get_account(&candidate.mint).await
            .context("Failed to fetch mint account")?;
        
        let mint = Mint::unpack(&account.data)
            .context("Failed to unpack mint account")?;
        
        // Pobierz metadane tokena (Metaplex)
        let metadata_uri = self.resolve_metadata_uri(&candidate.mint, rpc).await
            .unwrap_or_else(|_| "https://example.com/token.json".to_string());
        
        // Pobierz pełne metadane jeśli URI jest dostępne
        let metadata = if metadata_uri.starts_with("http") {
            self.fetch_metadata_from_uri(&metadata_uri).await.ok()
        } else {
            None
        };
        
        Ok((mint.supply, mint.decimals, metadata_uri, metadata))
    }

    async fn resolve_metadata_uri(&self, mint_address: &Pubkey, rpc: &RpcClient) -> Result<String> {
        // Implementacja pobierania URI metadanych z Metaplex Token Metadata Program
        // To jest uproszczona wersja - w rzeczywistości wymaga to znajdowania PDA
        // i deserializacji danych metadanych
        
        let metadata_program_id = Pubkey::from_str("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s")?;
        let seeds = &[
            b"metadata",
            metadata_program_id.as_ref(),
            mint_address.as_ref(),
        ];
        
        let (pda, _) = Pubkey::find_program_address(seeds, &metadata_program_id);
        
        match rpc.get_account(&pda).await {
            Ok(account) => {
                // Deserializuj dane metadanych (pomijamy dla uproszczenia)
                Ok("https://example.com/token.json".to_string())
            }
            Err(_) => {
                Err(anyhow!("Metadata account not found"))
            }
        }
    }

    async fn fetch_metadata_from_uri(&self, uri: &str) -> Result<Metadata> {
        let response = self.http_client.get(uri)
            .send()
            .await
            .context("Failed to fetch metadata")?;
        
        if !response.status().is_success() {
            return Err(anyhow!("Failed to fetch metadata: {}", response.status()));
        }
        
        let metadata: Metadata = response.json()
            .await
            .context("Failed to parse metadata")?;
        
        Ok(metadata)
    }

    async fn fetch_holder_distribution(&self, candidate: &PremintCandidate, rpc: &RpcClient) -> Result<Vec<HolderData>> {
        let largest_accounts = rpc.get_token_largest_accounts(&candidate.mint)
            .await
            .context("Failed to fetch token largest accounts")?;
        
        let total_supply = rpc.get_token_supply(&candidate.mint)
            .await
            .context("Failed to fetch token supply")?
            .amount
            .parse::<u64>()
            .context("Failed to parse token supply")?;
        
        let mut holders = Vec::new();
        
        for account in largest_accounts {
            let percentage = account.ui_amount_string
                .parse::<f64>()
                .unwrap_or(0.0) / (total_supply as f64 / 10f64.powi(9));
            
            let is_whale = percentage >= self.config.thresholds.whale_threshold;
            
            holders.push(HolderData {
                address: account.address,
                percentage,
                is_whale,
            });
        }
        
        Ok(holders)
    }

    async fn fetch_liquidity_data(&self, candidate: &PremintCandidate, rpc: &RpcClient) -> Result<Option<LiquidityPool>> {
        // Wyszukaj pule płynności na Raydium
        let raydium_pools = self.find_raydium_pools(candidate, rpc).await?;
        
        if let Some(pool) = raydium_pools.first() {
            return Ok(Some(pool.clone()));
        }
        
        // Wyszukaj pule na Pump.fun
        if let Some(pool) = self.find_pump_fun_pool(candidate, rpc).await? {
            return Ok(Some(pool));
        }
        
        Ok(None)
    }

    async fn find_raydium_pools(&self, candidate: &PremintCandidate, rpc: &RpcClient) -> Result<Vec<LiquidityPool>> {
        // Implementacja wyszukiwania puli Raydium
        // To jest uproszczona wersja - w rzeczywistości wymaga to analizy programów AMM
        Ok(vec![])
    }

    async fn find_pump_fun_pool(&self, candidate: &PremintCandidate, rpc: &RpcClient) -> Result<Option<LiquidityPool>> {
        // Implementacja wyszukiwania puli Pump.fun
        Ok(None)
    }

    async fn fetch_volume_data(&self, candidate: &PremintCandidate, rpc: &RpcClient) -> Result<VolumeData> {
        // Pobierz transakcje tokena
        let signatures = rpc.get_signatures_for_address(&candidate.mint)
            .await
            .context("Failed to fetch transaction signatures")?;
        
        let transaction_count = signatures.len() as u32;
        
        // Analizuj transakcje do obliczenia wolumenu
        let mut total_volume = 0.0;
        let mut buy_volume = 0.0;
        let mut sell_volume = 0.0;
        
        for signature_info in signatures.iter().take(100) { // Ogranicz do 100 tx
            if let Ok(transaction) = rpc.get_transaction(
                &signature_info.signature,
                rpc::config::RpcTransactionConfig {
                    encoding: Some(solana_transaction::UiTransactionEncoding::Json),
                    commitment: Some(CommitmentConfig::confirmed()),
                    max_supported_transaction_version: Some(0),
                }
            ).await {
                // Analiza transakcji do obliczenia wolumenu
                // Pomijamy pełną implementację dla uproszczenia
            }
        }
        
        let buy_sell_ratio = if sell_volume > 0.0 {
            buy_volume / sell_volume
        } else {
            1.0
        };
        
        Ok(VolumeData {
            initial_volume: 0.0,
            current_volume: total_volume,
            volume_growth_rate: 0.0,
            transaction_count,
            buy_sell_ratio,
        })
    }

    async fn fetch_creator_holdings(&self, candidate: &PremintCandidate, rpc: &RpcClient) -> Result<CreatorHoldings> {
        // Znajdź konto tokena twórcy
        let creator_token_accounts = rpc.get_token_accounts_by_owner(
            &candidate.creator,
            solana_client::rpc_request::TokenAccountsFilter::Mint(candidate.mint),
        ).await
        .context("Failed to fetch creator token accounts")?;
        
        let current_balance = if let Some(account) = creator_token_accounts.first() {
            account.account.data.parsed.info.token_amount.amount.parse::<u64>().unwrap_or(0)
        } else {
            0
        };
        
        // Śledź transakcje sprzedaży twórcy
        let sell_transactions = self.track_creator_sells(candidate, rpc).await?;
        
        Ok(CreatorHoldings {
            initial_balance: 0, // Wymaga śledzenia od początku
            current_balance,
            first_sell_timestamp: None, // Wymaga analizy historycznych transakcji
            sell_transactions,
        })
    }

    async fn track_creator_sells(&self, candidate: &PremintCandidate, rpc: &RpcClient) -> Result<u32> {
        // Implementacja śledzenia transakcji sprzedaży twórcy
        Ok(0)
    }

    async fn fetch_offchain_data(&self, candidate: &PremintCandidate) -> Result<()> {
        if let Some(api_key) = &self.config.pump_fun_api_key {
            let url = format!("https://api.pump.fun/token/{}", candidate.mint);
            let response = self.http_client.get(&url)
                .header("Authorization", format!("Bearer {}", api_key))
                .send()
                .await
                .context("Failed to fetch Pump.fun data")?;
            
            if response.status().is_success() {
                let data: Value = response.json().await?;
                debug!("Pump.fun data: {:?}", data);
            } else {
                warn!("Pump.fun API error: {}", response.status());
                let mut metrics = self.metrics.write().await;
                metrics.api_errors += 1;
            }
        }
        
        if let Some(api_key) = &self.config.bitquery_api_key {
            let query = json!({
                "query": format!(
                    "{{ solana {{ transfers(token: \"{}\") {{ count }} }} }}",
                    candidate.mint
                )
            });
            
            let response = self.http_client.post("https://graphql.bitquery.io")
                .header("X-API-KEY", api_key)
                .json(&query)
                .send()
                .await
                .context("Failed to fetch Bitquery data")?;
            
            if response.status().is_success() {
                let data: Value = response.json().await?;
                debug!("Bitquery data: {:?}", data);
            } else {
                warn!("Bitquery API error: {}", response.status());
                let mut metrics = self.metrics.write().await;
                metrics.api_errors += 1;
            }
        }
        
        Ok(())
    }

    async fn fetch_social_data(&self, candidate: &PremintCandidate) -> Result<SocialActivity> {
        // Implementacja pobierania danych społecznościowych
        // To może wymagać integracji z Twitter API, Discord API, itp.
        Ok(SocialActivity {
            twitter_mentions: 0,
            telegram_members: 0,
            discord_members: 0,
            social_score: 0.0,
        })
    }

    // 5. Obliczanie cech
    fn calculate_liquidity_score(&self, token_data: &TokenData) -> f64 {
        let liquidity = token_data.liquidity_pool.as_ref().map_or(0.0, |p| p.sol_amount);
        let normalized = liquidity / self.config.thresholds.min_liquidity_sol;
        min(normalized, 1.0)
    }

    fn calculate_holder_distribution_score(&self, token_data: &TokenData) -> f64 {
        if token_data.holder_distribution.is_empty() {
            return 0.0;
        }
        
        let top_holder = token_data.holder_distribution[0].percentage;
        let whale_count = token_data.holder_distribution.iter()
            .filter(|h| h.is_whale)
            .count();
        
        if top_holder < self.config.thresholds.whale_threshold && whale_count <= 1 {
            1.0
        } else {
            let whale_penalty = whale_count as f64 * 0.2;
            1.0 - (top_holder - self.config.thresholds.whale_threshold) / (1.0 - self.config.thresholds.whale_threshold) - whale_penalty
        }
    }

    fn calculate_volume_growth_score(&self, token_data: &TokenData) -> f64 {
        let growth = token_data.volume_data.volume_growth_rate;
        let normalized = growth / self.config.thresholds.volume_growth_threshold;
        min(normalized, 1.0)
    }

    fn calculate_holder_growth_score(&self, token_data: &TokenData) -> f64 {
        if token_data.holder_history.len() < 2 {
            return 0.5;
        }
        
        let initial = *token_data.holder_history.front().unwrap() as f64;
        let current = *token_data.holder_history.back().unwrap() as f64;
        
        if initial == 0.0 {
            return 0.5;
        }
        
        let growth = (current - initial) / initial;
        min(growth / self.config.thresholds.holder_growth_threshold, 1.0)
    }

    fn calculate_price_change_score(&self, token_data: &TokenData) -> f64 {
        if token_data.price_history.len() < 2 {
            return 0.5;
        }
        
        let initial_price = *token_data.price_history.front().unwrap();
        let current_price = *token_data.price_history.back().unwrap();
        
        if initial_price == 0.0 {
            return 0.5;
        }
        
        let change = (current_price - initial_price) / initial_price;
        
        if change > 0.0 {
            min(change, 1.0)
        } else {
            0.0
        }
    }

    fn calculate_creator_sell_score(&self, token_data: &TokenData, mint_timestamp: u64) -> f64 {
        if token_data.creator_holdings.sell_transactions > 0 {
            let sell_penalty = token_data.creator_holdings.sell_transactions as f64 * 0.1;
            return (1.0 - sell_penalty).max(0.0);
        }
        
        1.0
    }

    async fn calculate_metadata_score(&self, token_data: &TokenData) -> f64 {
        if let Some(metadata) = &token_data.metadata {
            let mut score = 0.0;

// Oceń nazwę
            if !metadata.name.is_empty() && metadata.name.len() <= 30 {
                score += 0.2;
            }
            
            // Oceń symbol
            if !metadata.symbol.is_empty() && metadata.symbol.len() <= 10 {
                score += 0.2;
            }
            
            // Oceń opis
            if !metadata.description.is_empty() && metadata.description.len() >= 50 {
                score += 0.3;
            }
            
            // Oceń obraz
            if metadata.image.starts_with("https://") {
                score += 0.2;
            }
            
            // Oceń atrybuty
            if !metadata.attributes.is_empty() {
                score += 0.1;
            }
            
            return score;
        }
        
        0.0
    }

    fn calculate_social_score(&self, token_data: &TokenData) -> f64 {
        let social = &token_data.social_activity;
        let mut score = 0.0;
        
        if social.twitter_mentions > 10 {
            score += 0.3;
        }
        
        if social.telegram_members > 100 {
            score += 0.3;
        }
        
        if social.discord_members > 100 {
            score += 0.4;
        }
        
        min(score, 1.0)
    }

    // 6. Obliczanie wyniku końcowego
    fn calculate_predicted_score(&self, feature_scores: &HashMap<String, f64>) -> u8 {
        let weights = &self.config.weights;
        let mut total_score = 0.0;
        
        total_score += feature_scores.get("liquidity").unwrap_or(&0.0) * weights.liquidity;
        total_score += feature_scores.get("holder_distribution").unwrap_or(&0.0) * weights.holder_distribution;
        total_score += feature_scores.get("volume_growth").unwrap_or(&0.0) * weights.volume_growth;
        total_score += feature_scores.get("holder_growth").unwrap_or(&0.0) * weights.holder_growth;
        total_score += feature_scores.get("price_change").unwrap_or(&0.0) * weights.price_change;
        total_score += feature_scores.get("jito_bundle_presence").unwrap_or(&0.0) * weights.jito_bundle_presence;
        total_score += feature_scores.get("creator_sell_speed").unwrap_or(&0.0) * weights.creator_sell_speed;
        total_score += feature_scores.get("metadata_quality").unwrap_or(&0.0) * weights.metadata_quality;
        total_score += feature_scores.get("social_activity").unwrap_or(&0.0) * weights.social_activity;
        
        let normalized = (total_score * 100.0).round().clamp(0.0, 100.0) as u8;
        
        if *feature_scores.get("jito_bundle_presence").unwrap_or(&0.0) > 0.5 {
            min(100, normalized + 5)
        } else {
            normalized
        }
    }

    fn generate_reason(&self, feature_scores: &HashMap<String, f64>, score: u8, anomaly_detected: bool) -> String {
        let mut reasons = Vec::new();
        
        if anomaly_detected {
            reasons.push("Anomaly detected - possible manipulation".to_string());
        }
        
        if let Some(&liquidity) = feature_scores.get("liquidity") {
            if liquidity > 0.8 {
                reasons.push("High liquidity".to_string());
            } else if liquidity < 0.3 {
                reasons.push("Low liquidity".to_string());
            }
        }
        
        if let Some(&holders) = feature_scores.get("holder_distribution") {
            if holders > 0.8 {
                reasons.push("Good holder distribution".to_string());
            } else if holders < 0.3 {
                reasons.push("High whale concentration".to_string());
            }
        }
        
        if let Some(&volume) = feature_scores.get("volume_growth") {
            if volume > 0.8 {
                reasons.push("Strong volume growth".to_string());
            }
        }
        
        if let Some(&creator) = feature_scores.get("creator_sell_speed") {
            if creator < 0.3 {
                reasons.push("Creator sold quickly".to_string());
            }
        }
        
        if let Some(&social) = feature_scores.get("social_activity") {
            if social > 0.7 {
                reasons.push("Strong social activity".to_string());
            }
        }
        
        if reasons.is_empty() {
            if score > 80 {
                "Exceptional token potential".to_string()
            } else if score > 60 {
                "Good token potential".to_string()
            } else {
                "Average token potential".to_string()
            }
        } else {
            format!("Score: {}. Factors: {}", score, reasons.join(", "))
        }
    }

    // 7. Integracja z GUI
    pub async fn send_to_gui(&self, scored: &ScoredCandidate) {
        let gui_data = json!({
            "mint": scored.base.mint.to_string(),
            "score": scored.predicted_score,
            "features": scored.feature_scores,
            "reason": scored.reason,
            "calculation_time": scored.calculation_time,
            "anomaly_detected": scored.anomaly_detected,
        });
        
        info!("GUI Update: {}", gui_data);
    }

    // 8. Anomaly detection
    fn detect_anomalies(&self, token_data: &TokenData) -> bool {
        let volume = &token_data.volume_data;
        
        // Wykrywanie nietypowego wolumenu
        if volume.volume_growth_rate > 10.0 {
            warn!("Suspicious volume growth: {}", volume.volume_growth_rate);
            return true;
        }
        
        // Wykrywanie nietypowej liczby transakcji
        if volume.transaction_count > 1000 {
            warn!("High transaction count: {}", volume.transaction_count);
            return true;
        }
        
        // Wykrywanie koncentracji u holderów
        if let Some(top_holder) = token_data.holder_distribution.first() {
            if top_holder.percentage > 0.5 {
                warn!("High top holder concentration: {}%", top_holder.percentage * 100.0);
                return true;
            }
        }
        
        // Wykrywanie szybkiej sprzedaży twórcy
        if token_data.creator_holdings.sell_transactions > 5 {
            warn!("Creator sold multiple times: {}", token_data.creator_holdings.sell_transactions);
            return true;
        }
        
        false
    }

    // 9. Metody utility
    pub async fn get_metrics(&self) -> OracleMetrics {
        self.metrics.read().await.clone()
    }
    
    pub async fn clear_cache(&self) {
        let mut cache = self.token_cache.write().await;
        cache.clear();
    }
    
    pub async fn get_cache_size(&self) -> usize {
        let cache = self.token_cache.read().await;
        cache.len()
    }
}

// Implementacja Clone dla PredictiveOracle
impl Clone for PredictiveOracle {
    fn clone(&self) -> Self {
        Self {
            candidate_receiver: self.candidate_receiver.clone(),
            scored_sender: self.scored_sender.clone(),
            gui_suggestions: self.gui_suggestions.clone(),
            rpc_clients: self.rpc_clients.clone(),
            http_client: self.http_client.clone(),
            config: self.config.clone(),
            token_cache: RwLock::new(HashMap::new()), // Nowa instancja cache
            metrics: self.metrics.clone(),
            rate_limiter: self.rate_limiter.clone(),
            request_semaphore: self.request_semaphore.clone(),
        }
    }
}
