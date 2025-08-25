const { ApiPromise, WsProvider } = require('@polkadot/api');
const { Keyring } = require('@polkadot/keyring');
const { BN } = require('@polkadot/util');

async function main() {
    const args = process.argv.slice(2);
    if (args.length < 3) {
        console.error('Usage: node batch-force-set-balance.js <node-url> <num-accounts> <sudo-seed> [balance] [batch-size] [check-balance] [query-batch-size]');
        process.exit(1);
    }

    const nodeUrl = args[0];
    const numAccounts = parseInt(args[1], 10);
    const sudoSeed = args[2];
    // Default to 100 tokens (assuming 18 decimals) + some for fees
    const balance = args[3] || '100000000000000000000';
    // Allow customizing batch size - default to a smaller batch size of 10 for better reliability
    const batchSize = parseInt(args[4] || '10', 10);
    // Whether to check balances (can be slow for many accounts)
    // Default to false for large account numbers
    const checkBalances = args[5] === 'true' || args[5] === 'yes' || args[5] === '1';
    // Query batch size for balance checks - default to 20 for better reliability
    const queryBatchSize = parseInt(args[6] || '1000', 10);

    // Connection options
    const wsOptions = {
        maxReconnects: 5,
        reconnectDelay: 1000
    };

    console.log(`Connecting to node: ${nodeUrl}...`);
    console.log(`Configuration:
- Number of accounts: ${numAccounts} (${numAccounts} senders + ${numAccounts} receivers)
- Balance to set: ${balance}
- Transaction batch size: ${batchSize}
- Check existing balances: ${checkBalances ? 'Yes' : 'No'}
- Query batch size: ${queryBatchSize}`);

    let api;
    let provider;
    
    try {
        provider = new WsProvider(nodeUrl, 10000, wsOptions); // 10 second timeout
        
        provider.on('error', (error) => {
            console.error(`WebSocket error: ${error.message}`);
        });

        provider.on('disconnected', () => {
            console.log('WebSocket disconnected. Attempting to reconnect...');
        });
        
        api = await ApiPromise.create({
            provider,
            throwOnConnect: true
        });
        
        // Display chain information
        const [chain, nodeName, nodeVersion, chainType] = await Promise.all([
            api.rpc.system.chain(),
            api.rpc.system.name(),
            api.rpc.system.version(),
            api.rpc.system.chainType()
        ]);
        
        console.log(`Connected to chain ${chain} (${chainType.toString()}) using ${nodeName} v${nodeVersion}`);
        
        // Check if forceSetBalance exists
        if (!api.tx.balances.forceSetBalance) {
            console.error('Error: balances.forceSetBalance is not available on this chain');
            await api.disconnect();
            process.exit(1);
        }
        
        // Parse the target balance
        const targetBalance = new BN(balance);
        
        // Check if sudo module exists
        if (!api.tx.sudo) {
            console.error('Error: sudo module is not available on this chain');
            await api.disconnect();
            process.exit(1);
        }
        
        // Create keyring instance
        const keyring = new Keyring({ type: 'sr25519' });
        const sudo = keyring.addFromUri(sudoSeed);
        
        // Check sudo balance
        const sudoAccount = await api.query.system.account(sudo.address);
        console.log(`Sudo account ${sudo.address} has ${sudoAccount.data.free.toHuman()} free balance`);
        
        // Check if the account is actually the sudo key
        const sudoKey = await api.query.sudo.key();
        if (sudoKey.toString() !== sudo.address) {
            console.log(`Warning: ${sudo.address} is not the sudo key. The sudo key is ${sudoKey.toString()}`);
        }
        
        // Use the correct derivation path format: {seed}/{i}
        const senderSeedPrefix = '//Sender';
        const receiverSeedPrefix = '//Receiver';
        const calls = [];
        
        // Create all the calls first, checking existing balances
        console.log('Preparing accounts and funding calls...');
        
        let accountsToFund = 0;
        let accountsSkipped = 0;
        
        // For large numbers of accounts, we'll skip balance checking by default
        if (numAccounts > 1000 && !checkBalances) {
            console.log(`Large number of accounts (${numAccounts * 2} total) detected. Skipping balance checks for better performance.`);
            console.log('To force balance checking, set the check-balance parameter to true.');
        }
        
        if (checkBalances) {
            // Generate all accounts first (both senders and receivers)
            console.log(`Generating ${numAccounts * 2} account addresses...`);
            const accounts = [];
            
            // Generate sender accounts
            for (let i = 0; i < numAccounts; i++) {
                const senderUri = `${senderSeedPrefix}/${i}`; // Using /{i} format instead of //{i}
                const sender = keyring.addFromUri(senderUri, { name: `sender ${i}` });
                accounts.push({
                    index: i,
                    type: 'sender',
                    address: sender.address,
                    uri: senderUri
                });
                
                // Log progress every 10000 accounts
                if (i > 0 && i % 10000 === 0) {
                    console.log(`Generated ${i} sender accounts...`);
                }
            }
            
            // Generate receiver accounts
            for (let i = 0; i < numAccounts; i++) {
                const receiverUri = `${receiverSeedPrefix}/${i}`; // Using /{i} format instead of //{i}
                const receiver = keyring.addFromUri(receiverUri, { name: `receiver ${i}` });
                accounts.push({
                    index: i,
                    type: 'receiver',
                    address: receiver.address,
                    uri: receiverUri
                });
                
                // Log progress every 10000 accounts
                if (i > 0 && i % 10000 === 0) {
                    console.log(`Generated ${i} receiver accounts...`);
                }
            }
            
            console.log(`Generated ${accounts.length} accounts (${numAccounts} senders + ${numAccounts} receivers)`);
            
            // Query balances in smaller batches for better reliability
            const totalBatches = Math.ceil(accounts.length / queryBatchSize);
            
            console.log(`Checking account balances in ${totalBatches} batches of ${queryBatchSize} accounts each...`);
            
            for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
                const start = batchIndex * queryBatchSize;
                const end = Math.min(start + queryBatchSize, accounts.length);
                const batchAccounts = accounts.slice(start, end);
                
                // Log progress every 10 batches or for the first and last batch
                if (batchIndex % 10 === 0 || batchIndex === 0 || batchIndex === totalBatches - 1) {
                    console.log(`Querying balances for accounts ${start} to ${end-1} (batch ${batchIndex + 1}/${totalBatches})...`);
                }
                
                try {
                    // Use queryMulti to get all balances at once
                    const addresses = batchAccounts.map(a => a.address);
                    const balances = await api.query.system.account.multi(addresses);
                    
                    for (let i = 0; i < batchAccounts.length; i++) {
                        const account = batchAccounts[i];
                        const currentBalance = balances[i];
                        
                        if (currentBalance.data.free.lt(targetBalance)) {
                            if (accountsToFund < 5) {
                                console.log(`${account.type.charAt(0).toUpperCase() + account.type.slice(1)} ${account.index}: ${account.address.slice(0, 10)}... needs funding. Current: ${currentBalance.data.free.toHuman()}`);
                            } else if (accountsToFund === 5) {
                                console.log(`... and more accounts need funding`);
                            }
                            calls.push(api.tx.balances.forceSetBalance(account.address, balance));
                            accountsToFund++;
                        } else {
                            accountsSkipped++;
                        }
                    }
                    
                    // Add a small delay between batches to avoid overwhelming the node
                    if (batchIndex < totalBatches - 1) {
                        await new Promise(resolve => setTimeout(resolve, 100));
                    }
                    
                    // Log progress every 100 batches
                    if ((batchIndex + 1) % 100 === 0) {
                        console.log(`Processed ${batchIndex + 1}/${totalBatches} batches. Found ${accountsToFund} accounts to fund so far.`);
                    }
                    
                    // Check if connection is still alive
                    if (batchIndex % 50 === 0 && batchIndex > 0) {
                        try {
                            await api.rpc.system.health();
                        } catch (error) {
                            console.log('Connection check failed. Attempting to reconnect...');
                            await api.disconnect();
                            
                            provider = new WsProvider(nodeUrl, 10000, wsOptions);
                            api = await ApiPromise.create({ provider });
                            console.log('Reconnected to node.');
                        }
                    }
                } catch (error) {
                    console.error(`Error querying batch ${batchIndex + 1}: ${error.message}`);
                    console.log('Adding all accounts in this batch to funding list to be safe.');
                    
                    // If query fails, add all accounts in the batch to be funded
                    for (const account of batchAccounts) {
                        calls.push(api.tx.balances.forceSetBalance(account.address, balance));
                        accountsToFund++;
                    }
                    
                    // Try to reconnect if needed
                    try {
                        await api.rpc.system.health();
                    } catch (reconnectError) {
                        console.log('Connection lost. Attempting to reconnect...');
                        await api.disconnect();
                        
                        provider = new WsProvider(nodeUrl, 10000, wsOptions);
                        api = await ApiPromise.create({ provider });
                        console.log('Reconnected to node.');
                    }
                }
            }
        } else {
            // Skip balance checking, just create calls for all accounts
            console.log('Balance checking disabled. Creating funding calls for all accounts...');
            
            // First fund sender accounts
            console.log(`Creating ${numAccounts} sender account funding calls...`);
            for (let i = 0; i < numAccounts; i++) {
                const senderUri = `${senderSeedPrefix}/${i}`; // Using /{i} format instead of //{i}
                const sender = keyring.addFromUri(senderUri, { name: `sender ${i}` });
                calls.push(api.tx.balances.forceSetBalance(sender.address, balance));
                accountsToFund++;
                
                // Log progress every 10000 accounts
                if (i > 0 && i % 10000 === 0) {
                    console.log(`Created ${i}/${numAccounts} sender funding calls...`);
                }
            }
            
            // Then fund receiver accounts
            console.log(`Creating ${numAccounts} receiver account funding calls...`);
            for (let i = 0; i < numAccounts; i++) {
                const receiverUri = `${receiverSeedPrefix}/${i}`; // Using /{i} format instead of //{i}
                const receiver = keyring.addFromUri(receiverUri, { name: `receiver ${i}` });
                calls.push(api.tx.balances.forceSetBalance(receiver.address, balance));
                accountsToFund++;
                
                // Log progress every 10000 accounts
                if (i > 0 && i % 10000 === 0) {
                    console.log(`Created ${i}/${numAccounts} receiver funding calls...`);
                }
            }
        }
        
        console.log(`\nSummary: ${accountsToFund} accounts need funding, ${accountsSkipped} accounts already have sufficient balance.`);
        
        if (calls.length === 0) {
            console.log('No accounts need funding. Exiting.');
            await api.disconnect();
            return;
        }
        
        // Split calls into chunks of batchSize
        const callChunks = [];
        for (let i = 0; i < calls.length; i += batchSize) {
            callChunks.push(calls.slice(i, i + batchSize));
        }
        
        console.log(`Created ${callChunks.length} batch(es) of up to ${batchSize} calls each.`);

        try {
            let nonce = await api.rpc.system.accountNextIndex(sudo.address);
            console.log(`Starting with nonce: ${nonce.toString()}`);

            // Track successful and failed batches
            let successfulBatches = 0;
            let failedBatches = 0;

            for (const [index, chunk] of callChunks.entries()) {
                console.log(`\nSending batch ${index + 1}/${callChunks.length} with ${chunk.length} calls (using nonce: ${nonce})...`);

                // Using `utility.batchAll` which requires all calls to succeed
                const batchCall = api.tx.utility.batchAll(chunk);
                const tx = api.tx.sudo.sudo(batchCall);

                // Debug info
                try {
                    const info = await tx.paymentInfo(sudo);
                    console.log(`Estimated transaction fee: ${info.partialFee.toHuman()}`);
                } catch (err) {
                    console.log(`Could not estimate fee: ${err.message}`);
                }

                let success = false;
                let retries = 0;
                const maxRetries = 3;
                
                while (!success && retries < maxRetries) {
                    try {
                        // Get fresh nonce before each attempt
                        if (retries > 0) {
                            nonce = await api.rpc.system.accountNextIndex(sudo.address);
                            console.log(`Using fresh nonce: ${nonce.toString()}`);
                        }
                        
                        await new Promise((resolve, reject) => {
                            let unsub;
                            let timeout;
                            
                            const cleanup = () => {
                                if (timeout) clearTimeout(timeout);
                                if (unsub) unsub();
                            };
                            
                            // Set a timeout for the transaction - 30 seconds
                            timeout = setTimeout(() => {
                                cleanup();
                                reject(new Error('Transaction timed out after 30 seconds'));
                            }, 30000);
                            
                            // Sign and send the transaction
                            tx.signAndSend(sudo, { nonce }, ({ status, events, dispatchError }) => {
                                console.log(`- Transaction status: ${status}`);

                                if (status.isInBlock || status.isFinalized) {
                                    cleanup();
                                    
                                    if (status.isInBlock) {
                                        console.log(`✅ Transaction included in block ${status.asInBlock.toHex()}`);
                                    } else {
                                        console.log(`✅ Transaction finalized in block ${status.asFinalized.toHex()}`);
                                    }
                                    
                                    // Consider transaction successful once it's in a block
                                    resolve();
                                } else if (status.isInvalid) {
                                    console.log("Transaction is invalid");
                                    cleanup();
                                    reject(new Error('Transaction is invalid'));
                                }
                            }).catch(err => {
                                cleanup();
                                reject(err);
                            });
                        });
                        
                        // If we get here, the transaction was successful
                        success = true;
                        successfulBatches++;
                        
                    } catch (err) {
                        console.error(`❌ Error in batch ${index + 1} (attempt ${retries + 1}/${maxRetries}): ${err.message}`);
                        
                        if (err.message.includes('Invalid Transaction') || err.message.includes('1010')) {
                            console.log('Transaction validation error. Refreshing nonce...');
                            // We'll get a fresh nonce on the next loop iteration
                        } else if (err.message.includes('timed out')) {
                            console.log('Transaction timed out. Checking if it was included in a block...');
                            
                            // Wait a moment before checking
                            await new Promise(resolve => setTimeout(resolve, 5000));
                            
                            // Check if nonce has increased
                            const newNonce = await api.rpc.system.accountNextIndex(sudo.address);
                            if (newNonce.gt(nonce)) {
                                console.log(`Transaction may have been included. Nonce increased from ${nonce} to ${newNonce}`);
                                nonce = newNonce;
                                success = true;
                                successfulBatches++;
                            } else {
                                console.log('Transaction was not included. Will retry...');
                            }
                        }
                        
                        retries++;
                        
                        // Add a delay before retrying
                        if (!success && retries < maxRetries) {
                            console.log(`Waiting 3 seconds before retry ${retries + 1}/${maxRetries}...`);
                            await new Promise(resolve => setTimeout(resolve, 3000));
                        }
                    }
                }
                
                if (!success) {
                    console.log(`Failed to send batch ${index + 1} after ${maxRetries} attempts. Continuing with next batch...`);
                    failedBatches++;
                }
                
                // Increment nonce for the next batch if the current one was successful
                if (success) {
                    // nonce = nonce.addn(1);
                }
                
                // Add a small delay between batches
                if (index < callChunks.length - 1) {
                    console.log('Waiting 2 seconds before next batch...');
                    await new Promise(resolve => setTimeout(resolve, 2000));
                }
            }
            
            console.log(`\n✅ All batches processed. ${successfulBatches} successful, ${failedBatches} failed.`);
            
        } catch (error) {
            console.error(`❌ Error: ${error.message}`);
        } finally {
            console.log('Disconnecting from node...');
            await api.disconnect();
        }
        
    } catch (error) {
        console.error(`❌ Connection error: ${error.message}`);
        if (api) {
            try {
                await api.disconnect();
            } catch (e) {
                // Ignore disconnect errors
            }
        }
        process.exit(1);
    }
}

main(); 