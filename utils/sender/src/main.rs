use clap::Parser;
use codec::Decode;
use futures::TryStreamExt;
use log::*;
use std::{
	collections::VecDeque,
	error::Error,
	sync::atomic::{AtomicU64, Ordering},
	time::Instant,
};
// use subxt::{ext::sp_core::Pair, utils::AccountId32, OnlineClient, PolkadotConfig};

use subxt::{
	blocks::BlockRef,
	config::polkadot::PolkadotExtrinsicParamsBuilder as Params,
	dynamic::Value,
	ext::sp_core::{sr25519::Pair as SrPair, Pair},
	tx::PairSigner,
	OnlineClient, PolkadotConfig,
};
use tokio::sync::RwLock;

const SENDER_SEED: &str = "//Sender";
const RECEIVER_SEED: &str = "//Receiver";
const ALICE_SEED: &str = "//Alice";

/// Util program to send transactions
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
	/// Node URL. Can be either a collator, or relaychain node based on whether you want to measure parachain TPS, or relaychain TPS.
	#[arg(long)]
	node_url: String,

	/// Total number of senders
	#[arg(long)]
	total_senders: Option<usize>,

	/// Chunk size for sending the extrinsics.
	#[arg(long, default_value_t = 50)]
	chunk_size: usize,

	/// Total number of pre-funded accounts (on funded-accounts.json).
	#[arg(long)]
	tps: usize,

	/// Send in batch mode with the batch size this large.
	#[arg(long, default_value_t = 1)]
	batch: usize,

	/// Seed the sender accounts
	#[arg(
		long,
        default_value_t = false,
        default_missing_value = "false",
        num_args = 0..=1,
        require_equals = false,
    )]
	seed: bool,
}

// FIXME: This assumes that all the chains supported by sTPS use this `AccountInfo` type. Currently,
// that holds. However, to benchmark a chain with another `AccountInfo` structure, a mechanism to
// adjust this type info should be provided.
type AccountInfo = frame_system::AccountInfo<u32, pallet_balances::AccountData<u128>>;

use jsonrpsee_client_transport::ws::WsTransportClientBuilder;
use jsonrpsee_core::client::{async_client::PingConfig, Client};
use std::sync::Arc;
use subxt::backend::legacy::LegacyBackend;

use tokio::time::Duration;

async fn get_account_nonce_with_retry<C: subxt::Config>(
	api: &OnlineClient<C>,
	block: BlockRef<C::Hash>,
	account: &SrPair,
	max_retries: u32,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
	for attempt in 0..max_retries {
		match get_account_nonce_inner(api, block.clone(), account).await {
			Ok(nonce) => return Ok(nonce),
			Err(e) => {
				if attempt < max_retries - 1 {
					log::warn!("Failed to get nonce (attempt {}): {:?}, retrying...", attempt + 1, e);
					tokio::time::sleep(Duration::from_millis(500 * (attempt + 1) as u64)).await;
				} else {
					return Err(e);
				}
			}
		}
	}
	unreachable!()
}

async fn get_account_nonce_inner<C: subxt::Config>(
	api: &OnlineClient<C>,
	block: BlockRef<C::Hash>,
	account: &SrPair,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
	let pubkey = account.public();
	let account_state_storage_addr = subxt::dynamic::storage(
		"System",
		"Account",
		vec![subxt::dynamic::Value::from_bytes(pubkey)],
	);

	let account_state_enc = api
		.storage()
		.at(block)
		.fetch(&account_state_storage_addr)
		.await?
		.ok_or("Nonce is not set")?
		.into_encoded();

	let account_state: AccountInfo =
		Decode::decode(&mut &account_state_enc[..]).map_err(|e| format!("Failed to decode account state: {}", e))?;
	Ok(account_state.nonce.into())
}

async fn get_account_nonce<C: subxt::Config>(
	api: &OnlineClient<C>,
	block: BlockRef<C::Hash>,
	account: &SrPair,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
	get_account_nonce_with_retry(api, block, account, 3).await
}

fn main() -> Result<(), Box<dyn Error>> {
	env_logger::init_from_env(
		env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
	);

	let args = Args::parse();

	// Assume number of senders equal to TPS if not specified.
	let n_sender_tasks = if args.batch > 1 { args.tps / args.batch } else { args.tps };
	let n_tx_sender = args.total_senders.unwrap_or(args.tps);
	// Limit concurrent tasks to prevent resource exhaustion
	const MAX_CONCURRENT_TASKS: usize = 1000;
	let n_sender_tasks = std::cmp::min(n_sender_tasks, MAX_CONCURRENT_TASKS);
	let worker_sleep =
		(1_000f64 * ((n_sender_tasks as f64 * args.batch as f64) / args.tps as f64)) as u64;

	log::info!("worker_sleep = {}", worker_sleep);
	log::info!("sender tasks  = {}", n_sender_tasks);
	log::info!("sender accounts  = {}", n_tx_sender);

	let sender_accounts = funder_lib::derive_accounts(n_tx_sender, SENDER_SEED.to_owned());
	let receiver_accounts = funder_lib::derive_accounts(n_tx_sender, RECEIVER_SEED.to_owned());
	let alice = <SrPair as Pair>::from_string(&ALICE_SEED, None).unwrap();
	let alice_signer = PairSigner::<PolkadotConfig, SrPair>::new(alice.clone());

	async fn create_api(node_url: String) -> Result<OnlineClient<PolkadotConfig>, Box<dyn std::error::Error + Send + Sync>> {
		let node_url = url::Url::parse(&node_url)?;
		
		for attempt in 0..5 {
			match create_api_inner(node_url.clone()).await {
				Ok(api) => return Ok(api),
				Err(e) => {
					log::warn!("Failed to create API connection (attempt {}): {:?}", attempt + 1, e);
					if attempt < 4 {
						tokio::time::sleep(Duration::from_secs(2 * (attempt + 1) as u64)).await;
					}
				}
			}
		}
		
		Err("Failed to create API connection after 5 attempts".into())
	}
	
	async fn create_api_inner(node_url: url::Url) -> Result<OnlineClient<PolkadotConfig>, Box<dyn std::error::Error + Send + Sync>> {
		let (node_sender, node_receiver) =
			WsTransportClientBuilder::default()
				.max_request_size(32 * 1024 * 1024)
				.max_response_size(32 * 1024 * 1024)
				.connection_timeout(Duration::from_secs(30))
				.build(node_url.clone()).await?;
		let client = Client::builder()
			.request_timeout(Duration::from_secs(60))
			.max_buffer_capacity_per_subscription(64 * 1024 * 1024)
			.enable_ws_ping(PingConfig::new()
				.ping_interval(Duration::from_secs(30))
				.inactive_limit(Duration::from_secs(180)))
			.set_tcp_no_delay(true)
			.max_concurrent_requests(1024)
			.build_with_tokio(node_sender, node_receiver);
		let backend = Arc::new(LegacyBackend::builder().build(client));
		Ok(OnlineClient::from_backend(backend).await?)
	}

	if args.seed {
		log::info!("Seeding accounts");

		tokio::runtime::Builder::new_multi_thread()
			.enable_all()
			.build()
			.unwrap()
			.block_on(async {
				let node_url = args.node_url.clone();
				let api = match create_api(node_url.clone()).await {
					Ok(api) => api,
					Err(e) => {
						log::error!("Failed to create API for seeding: {:?}", e);
						std::process::exit(1);
					}
				};
				let mut best_block_stream =
					api.blocks().subscribe_best().await.expect("Subscribe to best block failed");
				let best_block = best_block_stream.next().await.unwrap().unwrap();
				let block_ref: BlockRef<subxt::utils::H256> =
					BlockRef::from_hash(best_block.hash());

				let mut nonce = match get_account_nonce(&api, block_ref.clone(), &alice).await {
					Ok(n) => n,
					Err(e) => {
						log::error!("Failed to get alice nonce: {:?}", e);
						0
					}
				};

				for sender in sender_accounts.iter() {
					let payload = subxt::dynamic::tx(
						"Balances",
						"transfer_keep_alive",
						vec![
							Value::unnamed_variant("Id", [Value::from_bytes(sender.public())]),
							Value::u128(100000000000000000000),
						],
					);

					let tx_params = Params::new().nonce(nonce as u64).build();

					let tx =
						api.tx().create_signed_offline(&payload, &alice_signer, tx_params).unwrap();

					let _watch = match tx.submit_and_watch().await {
						Ok(watch) => {
							log::info!("Seeded account");
							nonce += 1;
							watch
						},
						Err(err) => {
							log::warn!("{:?}", err);
							continue;
						},
					};
				}
			});
	}

	while !args.seed {
		let result: Result<(), Box<dyn std::error::Error + Send + Sync>> = tokio::runtime::Builder::new_multi_thread()
			.enable_all()
			.build()
			.unwrap()
			.block_on(async {
				let node_url = args.node_url.clone();
				let api = match create_api(node_url.clone()).await {
					Ok(api) => api,
					Err(e) => {
						log::error!("Failed to create API: {:?}, retrying in 5 seconds...", e);
						tokio::time::sleep(Duration::from_secs(5)).await;
						return Err(e);
					}
				};

				// Subscribe to best block stream
				let mut best_block_stream = match api.blocks().subscribe_best().await {
					Ok(stream) => stream,
					Err(e) => {
						log::error!("Failed to subscribe to best blocks: {:?}, retrying...", e);
						return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
					}
				};
				let first_block = match best_block_stream.next().await {
					Some(Ok(block)) => block,
					_ => {
						log::error!("Failed to get first block");
						return Err("Failed to get first block".into());
					}
				};
				let best_block = Arc::new(RwLock::new((first_block, Instant::now())));

				log::info!("Current best block: {}", best_block.read().await.0.number() );

				let sender_signers = sender_accounts
					.iter()
					.cloned()
					.map(PairSigner::<PolkadotConfig, SrPair>::new)
					.collect::<Vec<_>>();

				info!("Starting senders");

				// Overall metrics that we use to throttle
				// Transactions sent since last block
				let sent = Arc::new(AtomicU64::default());
				// Number of in block transactions.
				let in_block = Arc::new(AtomicU64::default());

				let mut timestamp = Duration::from_micros(0);
				let mut block_time = Duration::from_micros(0);

				loop {
					// Clear previous handles to prevent memory leak
					let mut handles = Vec::new();

					sent.store(0, Ordering::SeqCst);
					in_block.store(0, Ordering::SeqCst);

					// Spawn 1 task per sender with proper bounds checking.
				for i in 0..n_sender_tasks {
					let in_block = in_block.clone();
					let sent = sent.clone();

					// Ensure we have enough accounts for this sender
					if i >= sender_accounts.len() {
						log::warn!("Not enough sender accounts for task {}, skipping", i);
						break;
					}

					let sender = sender_accounts[i].clone();
					let signer: PairSigner<PolkadotConfig, SrPair> = sender_signers[i].clone();
					let _alice_signer = PairSigner::<PolkadotConfig, SrPair>::new(alice.clone());
					let _alice = alice.clone();
					let best_block = best_block.clone();
					let sent = sent.clone();
					let in_block = in_block.clone();

					let api = api.clone();
					let nrecv = if args.batch > 1 { args.batch } else { 1 };
					let receiver_accounts = receiver_accounts.clone();

					let task = async move {
						// Slowly ramp up 10ms slots.
						tokio::time::sleep(std::time::Duration::from_millis(((n_sender_tasks - i)*10) as u64)).await;

						// Fix: Ensure we don't exceed receiver_accounts bounds
						let receiver_start = i % receiver_accounts.len();
						let receiver_end = std::cmp::min(receiver_start + nrecv, receiver_accounts.len());
						let actual_batch_size = receiver_end - receiver_start;
						let receivers = &receiver_accounts[receiver_start..receiver_end];
						let mut sleep_time_ms = 0u64;
						let block_ref: BlockRef<subxt::utils::H256> = BlockRef::from_hash(best_block.read().await.0.hash());
						let mut nonce = loop {
							match get_account_nonce(&api, block_ref.clone(), &sender).await {
								Ok(n) => break n,
								Err(e) => {
									let err_str = format!("{:?}", e);
									log::error!("Failed to get sender nonce: {}", err_str);
									if err_str.contains("RestartNeeded") {
										log::error!("Connection needs restart, exiting task");
										return; // Exit the task
									}
									tokio::time::sleep(std::time::Duration::from_secs(1)).await;
								}
							}
						};

						loop {
								// Throttle if the backlog of un included txs is too high 
								if sent.load(Ordering::SeqCst) > in_block.load(Ordering::SeqCst) + 100_000 {
									// Wait 10ms and check again.
									tokio::time::sleep(std::time::Duration::from_millis(10)).await;
									// Substract above sleep from TPS delay.
									sleep_time_ms = sleep_time_ms.saturating_sub(10);
									continue
								}

								// Target a rate per worker, so we wait.
								tokio::time::sleep(std::time::Duration::from_millis(sleep_time_ms)).await;
								let now = Instant::now();
							log::debug!("Sender {} using nonce {}", i, nonce);

							let tx_payload = if args.batch > 1 && actual_batch_size > 1 {
								let calls = (0..actual_batch_size).map(|j|
									subxt::dynamic::tx(
										"Balances",
										"transfer_keep_alive",
										vec![
											Value::unnamed_variant("Id", [Value::from_bytes(receivers[j % receivers.len()].public())]),
											Value::u128(1000000000000),
										],
									).into_value()
								).collect::<Vec<_>>();
									subxt::dynamic::tx(
										"Utility",
										"batch",
										vec![ Value::named_composite(vec![("calls", calls.into())]) ]
									)
								} else {
									subxt::dynamic::tx(
										"Balances",
										"transfer_keep_alive",
										vec![
											Value::unnamed_variant("Id", [Value::from_bytes(receivers[0].public())]),
											Value::u128(1000000000000),
										],
									)
							};
							log::debug!("Sender {} using nonce {}", i, nonce);
							let tx_params = Params::new().nonce(nonce as u64).build();

							let tx = api
									.tx()
									.create_signed_offline(&tx_payload, &signer, tx_params)
									.unwrap();

							match tx.submit_and_watch().await {
								Ok(_watch) => {},
								Err(err) => {
									let err_str = format!("{:?}", err);
									log::warn!("Transaction submission failed: {}", err_str);
									
									// Check if connection needs restart
									if err_str.contains("RestartNeeded") {
										log::error!("Connection needs restart, exiting task");
										return; // Exit the task
									}
									
									// Check if it's a connection error
									if err_str.contains("ClientError") || err_str.contains("RequestTimeout") {
										log::warn!("Connection issue detected, waiting before retry...");
										tokio::time::sleep(std::time::Duration::from_secs(2)).await;
									}
									
									let block_ref: BlockRef<subxt::utils::H256> = BlockRef::from_hash(best_block.read().await.0.hash());
									nonce = match get_account_nonce(&api, block_ref, &sender).await {
										Ok(n) => n,
										Err(e) => {
											let err_str = format!("{:?}", e);
											log::error!("Failed to refresh nonce: {}", err_str);
											if err_str.contains("RestartNeeded") {
												log::error!("Connection needs restart, exiting task");
												return; // Exit the task
											}
											// Wait and retry from beginning
											tokio::time::sleep(std::time::Duration::from_secs(5)).await;
											0
										}
									};
									// at most 1 second
									sleep_time_ms = worker_sleep.saturating_sub(now.elapsed().as_millis() as u64);
									continue
								}
							};

							sent.fetch_add(actual_batch_size as u64, Ordering::SeqCst);
							// Determine how much left to sleep, we need to retry in 1000ms (backoff)
							sleep_time_ms = worker_sleep.saturating_sub(now.elapsed().as_millis() as u64);
							nonce += 1;
						}
					};
					handles.push(tokio::spawn(task));
				}

					log::info!("All senders started");

					let mut tps_window = VecDeque::new();
					let loop_start = Instant::now();

					loop {
						if let Ok(Some(new_best_block)) = best_block_stream.try_next().await {
							*best_block.write().await = (new_best_block, Instant::now());
						} else {
					log::error!("Best block subscription lost, trying to reconnect ... ");
					// When connection is lost, we need to restart the entire loop
					// to create a new API client
					log::warn!("Connection lost, restarting entire main loop...");
					// Abort all tasks
					for handle in handles.iter() {
						handle.abort();
					}
					// Return error to restart the main loop
					return Err("Connection lost, need to restart".into());						}

					let best_block = &best_block.read().await.0;
					let Ok(extrinsics) = best_block.extrinsics().await else {
						// Most likely, need to reconnect to RPC.
						log::warn!("Failed to fetch extrinsics, skipping block");
						continue
					};

					let mut txcount = 0;

					for ex in extrinsics.iter() {
						let pallet_name = match ex.pallet_name() {
							Ok(name) => name,
							Err(_) => continue,
						};
						let variant_name = match ex.variant_name() {
							Ok(name) => name,
							Err(_) => continue,
						};

						match (pallet_name, variant_name) {
							("Timestamp", "set") => {
								if let Ok(compact) = codec::Compact::<u64>::decode(&mut &ex.field_bytes()[..]) {
									let new_timestamp = Duration::from_millis(compact.0);
									block_time = new_timestamp - timestamp;
									timestamp = new_timestamp;
								}
							},
							("Nfts", "transfer") => {
								txcount += 1;
							},
							_ => (),
						}
					}
					match best_block.events().await {
						Ok(events) => {
							for ev in events.iter() {
								if let Ok(ev) = ev {
									match (ev.pallet_name(), ev.variant_name()) {
										("Balances", "Transfer") => {
											txcount += 1;
										},
										_ => (),
									}
								}
							}
						},
						Err(e) => {
							log::warn!("Failed to fetch events: {:?}", e);
						}
					}
						in_block.fetch_add(txcount , Ordering::SeqCst);
						let btime = if block_time.is_zero() { 6000 } else { block_time.as_millis() };
						let tps = txcount * 1000 / btime as u64;
						tps_window.push_back(tps as usize);

						// A window of size 12
						if tps_window.len() > 12 {
							tps_window.pop_front();
							let avg_tps = tps_window.iter().sum::<usize>();
							if avg_tps < args.tps / 4 {
								log::warn!("TPS dropped below 25% of target ...");
								break;
							}
						}

				let avg_tps = tps_window.iter().sum::<usize>() / tps_window.len();

				log::info!("TPS: {} \t | Avg: {} \t | Sent/Exec: {}/{} | Best: {} | txs = {} | block time = {:?}", tps, avg_tps, sent.load(Ordering::SeqCst),  in_block.load(Ordering::SeqCst), best_block.number(), txcount, block_time);
				
				// Check if tasks are still alive
				let alive_count = handles.iter().filter(|h| !h.is_finished()).count();
				if alive_count < n_sender_tasks / 2 {
					log::warn!("More than half of the tasks have died, restarting...");
					break;
				}
				
				if loop_start.elapsed() > Duration::from_secs(60 * 5) {
					break;
				}					}

					// Restarting
					for handle in handles.iter() {
						handle.abort();
					}
					log::info!("Restarting senders");
				}
			});
		
				// If the async block returned an error, wait and retry
		match result {
			Err(e) => {
				log::warn!("Main loop encountered an error: {:?}, waiting before restart...", e);
				std::thread::sleep(std::time::Duration::from_secs(5));
			}
			Ok(_) => {
				log::info!("Main loop completed normally");
			}
		}	}
	Ok(())
}
