import requests
import sys
import json
import time
import os
import shutil


class CoinAggregator:
    def __init__(self):
        self.chains = {
            "ethereum": "ethereum",
            "solana": "solana",
            "base": "base",
            "arbitrum": "arbitrum-one",
            "optimism": "optimistic-ethereum",
            "polygon": "polygon-pos",
            "binance-smart-chain": "binance-smart-chain",
            "sui": "sui",
            "avalanche": "avalanche",
            "unichain": "unichain",
        }

        # Alchemy supported networks mapping
        self.alchemy_networks = {
            "ethereum": "eth-mainnet",
            "polygon": "polygon-mainnet",
            "optimism": "opt-mainnet",
            "arbitrum": "arb-mainnet",
            "base": "base-mainnet",
            "avalanche": "avax-mainnet",
            "binance-smart-chain": "bnb-mainnet",
            "polygon": "polygon-mainnet",
            "unichain": "unichain-mainnet",
            "solana": "solana-mainnet",
        }

        # Set rate limiting for CoinGecko (per their guidelines)
        self.coingecko_wait = 2.0  # seconds between API calls (30 calls per minute)
        self.rate_limit_wait = 60  # seconds to wait after hitting rate limit

        self.alchemy_wait = 0.05  # 50ms between calls (20 calls per second)

        self.coingecko_api_key = os.environ.get("COINGECKO_API_KEY")
        if not self.coingecko_api_key:
            print("No COINGECKO_API_KEY found in environment variables.")
            sys.exit(1)

        self.alchemy_api_key = os.environ.get("ALCHEMY_API_KEY")
        if not self.alchemy_api_key:
            print("No ALCHEMY_API_KEY found in environment variables.")
            sys.exit(1)

        # Create output directory structure
        self.base_dir = os.getcwd()
        os.makedirs(self.base_dir, exist_ok=True)

        for chain in self.chains:
            chain_dir = os.path.join(self.base_dir, chain)
            os.makedirs(chain_dir, exist_ok=True)
            os.makedirs(os.path.join(chain_dir, "pngs"), exist_ok=True)

        self.logs_dir = os.path.join(self.base_dir, "logs")
        os.makedirs(self.logs_dir, exist_ok=True)

        self.COINS_LIST_URL = (
            "https://api.coingecko.com/api/v3/coins/list?include_platform=true"
        )
        self.COINS_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"
        self.headers = {
            "Accept": "application/json",
            "x-cg-demo-api-key": self.coingecko_api_key,
        }

    def process_chains(self):
        """Process all chains using the new approach."""
        # Step 1: Fetch all coins with platforms
        coins_with_platforms = self.fetch_coins_list_with_platforms()
        if not coins_with_platforms:
            print("Error: Could not fetch coins list. Aborting.")
            return

        # Step 2: Fetch all available coins by market cap
        all_coins_by_market_cap = self.fetch_all_coins_by_market_cap()
        if not all_coins_by_market_cap:
            print("Error: Could not fetch coins by market cap. Aborting.")
            return

        # Step 3: Create buckets for each chain
        chain_buckets = {chain: [] for chain in self.chains}

        # Track already added coins per chain to prevent duplicates
        chain_added_ids = {chain: set() for chain in self.chains}

        # Step 4: Fill buckets based on platform affiliations
        print("Filling chain buckets based on platform affiliations...")
        for coin in all_coins_by_market_cap:
            coin_id = coin["id"]
            for chain_name, platform_name in self.chains.items():
                # Skip if this coin is already in this chain's bucket
                if coin_id in chain_added_ids[chain_name]:
                    continue

                contract_address = self.has_contract_on_platform(
                    coin_id, coins_with_platforms, platform_name
                )
                if contract_address:
                    coin_info = {
                        "id": coin.get("id"),
                        "symbol": coin.get("symbol"),
                        "name": coin.get("name"),
                        "market_cap": coin.get("market_cap"),
                        "current_price": coin.get("current_price"),
                        "contract_address": contract_address,
                        "chain": chain_name,
                        "image_url": coin.get("image"),
                        "local_image": f"{coin.get('id')}.png",
                    }
                    chain_buckets[chain_name].append(coin_info)
                    # Mark this coin as added to this chain
                    chain_added_ids[chain_name].add(coin_id)

        # Step 5: Process each chain bucket
        for chain_name, tokens in chain_buckets.items():
            print(f"\nProcessing {chain_name}...")
            print(f"Found {len(tokens)} tokens for {chain_name}")

            # Limit to top 100 tokens (already sorted by market cap)
            top_tokens = tokens[:100]

            # Get current IDs for comparison with previous data
            current_ids = [token["id"] for token in top_tokens]

            # Clean up tokens that are no longer in the list
            self.clean_removed_tokens(chain_name, current_ids)

            # Download images for new tokens
            self.fetch_token_images(chain_name, top_tokens)

            # Enrich with metadata
            enriched_tokens = self.enrich_with_metadata(chain_name, top_tokens)
            top_tokens = enriched_tokens

            # Remove Uneeded Fields
            final_tokens = self.finalise_and_clean_up_tokens(top_tokens)

            # Save the data.json file
            data_path = os.path.join(self.base_dir, chain_name, "data.json")
            with open(data_path, "w", encoding="utf-8") as f:
                json.dump(final_tokens, f, indent=2, ensure_ascii=False)

            print(f"Saved data for {len(final_tokens)} tokens to {data_path}")

            # Add a shorter delay before processing the next chain
            if chain_name != list(self.chains.keys())[-1]:
                print(f"Waiting 1 seconds before processing the next chain...")
                time.sleep(1)

    def process_single_chain(self, chain_name):
        """Process a single chain using the new approach."""
        if chain_name not in self.chains:
            print(f"Chain '{chain_name}' not found in supported chains.")
            return

        platform_name = self.chains[chain_name]

        # Step 1: Fetch all coins with platforms
        coins_with_platforms = self.fetch_coins_list_with_platforms()
        if not coins_with_platforms:
            print("Error: Could not fetch coins list. Aborting.")
            return

        # Step 2: Fetch all available coins by market cap
        all_coins_by_market_cap = self.fetch_all_coins_by_market_cap()
        if not all_coins_by_market_cap:
            print("Error: Could not fetch coins by market cap. Aborting.")
            return

        # Step 3: Create bucket for the chain
        chain_bucket = []

        # Track already added coin IDs to prevent duplicates
        added_coin_ids = set()

        # Step 4: Fill bucket based on platform affiliations
        print(f"Finding tokens for {chain_name} on platform {platform_name}...")
        for coin in all_coins_by_market_cap:
            coin_id = coin["id"]

            # Skip if this coin is already in the bucket
            if coin_id in added_coin_ids:
                continue

            contract_address = self.has_contract_on_platform(
                coin_id, coins_with_platforms, platform_name
            )
            if contract_address:
                # Add coin to this chain's bucket
                coin_info = {
                    "id": coin.get("id"),
                    "symbol": coin.get("symbol"),
                    "name": coin.get("name"),
                    "market_cap": coin.get("market_cap"),
                    "current_price": coin.get("current_price"),
                    "contract_address": contract_address,
                    "chain": chain_name,
                    "image_url": coin.get("image"),
                    "local_image": f"{coin.get('id')}.png",
                }
                chain_bucket.append(coin_info)
                # Mark this coin as added
                added_coin_ids.add(coin_id)

        # Step 5: Process the chain bucket
        print(f"Found {len(chain_bucket)} tokens for {chain_name}")

        # Limit to top 100 tokens (already sorted by market cap)
        top_tokens = chain_bucket[:100]

        # Get current IDs for comparison with previous data
        current_ids = [token["id"] for token in top_tokens]

        # Clean up tokens that are no longer in the list
        self.clean_removed_tokens(chain_name, current_ids)

        # Download images for new tokens
        self.fetch_token_images(chain_name, top_tokens)

        # Enrich with metadata
        enriched_tokens = self.enrich_with_metadata(chain_name, top_tokens)
        top_tokens = enriched_tokens

        # Remove Uneeded Fields
        final_tokens = self.finalise_and_clean_up_tokens(top_tokens)

        # Save the data.json file
        data_path = os.path.join(self.base_dir, chain_name, "data.json")
        with open(data_path, "w", encoding="utf-8") as f:
            json.dump(final_tokens, f, indent=2, ensure_ascii=False)

        print(f"Saved data for {len(final_tokens)} tokens to {data_path}")

    def fetch_coins_list_with_platforms(self):
        """Fetch the complete list of coins with platform details."""
        print("Fetching complete coin list with platform details...")
        return self.fetch_data_from_coin_gecko_with_retry(self.COINS_LIST_URL)

    def fetch_all_coins_by_market_cap(self):
        """Fetch all available pages of coins sorted by market cap."""
        print("Fetching all coins sorted by market cap (this may take a while)...")
        all_coins = []
        page = 1

        while True:
            print(f"Fetching market data: page {page}...")
            params = {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": 250,
                "page": page,
                "sparkline": False,
            }

            coins_page = self.fetch_data_from_coin_gecko_with_retry(
                self.COINS_MARKETS_URL, params
            )
            if not coins_page or len(coins_page) == 0:
                print(
                    "No more coins returned from API - reached the end of available data"
                )
                break

            all_coins.extend(coins_page)
            print(f"Collected {len(all_coins)} coins so far")

            page += 1

            if page % 5 == 0:
                checkpoint_file = os.path.join(
                    self.logs_dir, "market_cap_checkpoint.json"
                )
                checkpoint_data = [
                    {
                        "id": coin.get("id"),
                        "symbol": coin.get("symbol"),
                        "name": coin.get("name"),
                        "market_cap": coin.get("market_cap"),
                    }
                    for coin in all_coins
                ]

                with open(checkpoint_file, "w") as f:
                    json.dump(checkpoint_data, f)
                print(f"Saved checkpoint with {len(all_coins)} coins")

        print(f"Total coins collected by market cap: {len(all_coins)}")
        return all_coins

    def fetch_data_from_coin_gecko_with_retry(self, url, params=None, max_retries=5):
        """Fetch data from CoinGecko with a retry mechanism."""
        for i in range(max_retries):
            try:
                response = requests.get(url, headers=self.headers, params=params)

                if response.status_code == 429:
                    print(f"Rate limit hit. Waiting {self.rate_limit_wait} seconds...")
                    time.sleep(self.rate_limit_wait)
                    continue

                response.raise_for_status()
                time.sleep(self.coingecko_wait)
                return response.json()
            except requests.RequestException as e:
                print(f"Request failed (attempt {i+1}/{max_retries}): {e}")
                if i < max_retries - 1:
                    wait_time = self.coingecko_wait + (10 * (i + 1))
                    print(f"Retrying in {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Max retries reached for: {url}")
                    raise
        return None

    def has_contract_on_platform(self, coin_id, coins_with_platforms, platform_name):
        """Check if a coin has a contract on the specified platform."""
        for coin in coins_with_platforms:
            if coin["id"] == coin_id:
                if coin.get("platforms", {}).get(platform_name):
                    return coin["platforms"][platform_name]
                elif (
                    coin_id == platform_name.replace("-", "_")
                    or coin_id == platform_name
                ):
                    return "native"
        return None

    def enrich_with_metadata(self, chain_name, tokens):
        print(f"Enriching {len(tokens)} {chain_name} tokens with metadata...")
        enriched_count = 0
        skipped_count = 0

        for i, token in enumerate(tokens):
            contract_address = token.get("contract_address")
            if not contract_address or contract_address == "native":
                skipped_count += 1
                continue

            if token.get("metadata"):
                print(f"Token {token.get('name', 'Unknown')} already has metadata")
                enriched_count += 1
                continue

            metadata = self.fetch_metadata(chain_name, contract_address)
            if metadata:
                token["metadata"] = metadata
                enriched_count += 1
                print(
                    f"Enriched [{i+1}/{len(tokens)}] {token.get('name')} ({token.get('symbol')})"
                )
            else:
                print(
                    f"No metadata found for [{i+1}/{len(tokens)}] {token.get('name')} ({token.get('symbol')})"
                )

        print(
            f"Summary: Enriched {enriched_count} tokens, skipped {skipped_count} tokens, failed {len(tokens) - enriched_count - skipped_count} tokens"
        )
        return tokens

    def fetch_metadata(self, chain_name, contract_address):
        """
        Fetch token metadata from relevant API.
        """

        if not contract_address or contract_address == "native":
            return None

        headers = {"Accept": "application/json", "Content-Type": "application/json"}

        # For SUI
        if chain_name == "sui":

            url = "https://sui-mainnet-endpoint.blockvision.org"

            time.sleep(2)

            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "suix_getCoinMetadata",
                "params": [contract_address],
            }

            try:
                response = requests.post(url, headers=headers, json=payload)

                if response.status_code == 429:
                    print(f"SUI RPC rate limit hit. Waiting 2 seconds...")
                    time.sleep(5)
                    response = requests.post(url, headers=headers, json=payload)
                    if response.status_code == 429:
                        print("Rate limit hit again, skipping this token")
                        return None

                response.raise_for_status()

                result = response.json()

                if "result" in result:
                    metadata = result["result"]

                    formatted_metadata = {
                        "name": metadata.get("name"),
                        "symbol": metadata.get("symbol"),
                        "decimals": metadata.get("decimals"),
                        "description": metadata.get("description"),
                        "iconUrl": metadata.get("iconUrl"),
                        "id": metadata.get("id"),
                    }

                    return formatted_metadata

                return None

            except Exception as e:
                print(f"Error fetching SUI metadata for {contract_address}: {e}")
                self.log_error("metadata", f"sui:{contract_address}", str(e))
                return None

        # EVM chains, Solana
        else:

            network = self.alchemy_networks[chain_name]
            url = f"https://{network}.g.alchemy.com/v2/{self.alchemy_api_key}"

            # Solana
            if chain_name == "solana":
                payload = {
                    "id": 1,
                    "jsonrpc": "2.0",
                    "method": "getTokenSupply",
                    "params": [contract_address],
                }

                try:
                    response = requests.post(url, headers=headers, json=payload)

                    if response.status_code == 429:
                        print(f"Solana API rate limit hit. Waiting 2 seconds...")
                        time.sleep(2)
                        response = requests.post(url, headers=headers, json=payload)
                        if response.status_code == 429:
                            print("Rate limit hit again, skipping this token")
                            return None

                    response.raise_for_status()
                    time.sleep(self.alchemy_wait)

                    result = response.json()

                    # Extract decimals from token supply response
                    decimals = None
                    if "result" in result and "value" in result["result"]:
                        decimals = result["result"]["value"].get("decimals")

                    formatted_metadata = {
                        "decimals": decimals,
                    }

                    return formatted_metadata

                except Exception as e:
                    print(f"Error fetching Solana decimals for {contract_address}: {e}")
                    self.log_error("metadata", f"solana:{contract_address}", str(e))
                    return None

            # EVM
            else:
                payload = {
                    "id": 1,
                    "jsonrpc": "2.0",
                    "method": "alchemy_getTokenMetadata",
                    "params": [contract_address],
                }

                try:
                    response = requests.post(url, headers=headers, json=payload)

                    if response.status_code == 429:
                        print(f"Alchemy rate limit hit. Waiting 2 seconds...")
                        time.sleep(2)
                        response = requests.post(url, headers=headers, json=payload)
                        if response.status_code == 429:
                            print("Rate limit hit again, skipping this token")
                            return None

                    response.raise_for_status()

                    time.sleep(self.alchemy_wait)

                    result = response.json()
                    if "result" in result:
                        metadata = result["result"]

                        formatted_metadata = {
                            "name": metadata.get("name"),
                            "symbol": metadata.get("symbol"),
                            "decimals": metadata.get("decimals"),
                            "logo": metadata.get("logo"),
                        }

                        if "totalSupply" in metadata:
                            formatted_metadata["totalSupply"] = metadata["totalSupply"]

                        return formatted_metadata

                    return None
                except Exception as e:
                    print(
                        f"Error fetching Alchemy metadata for {chain_name}:{contract_address}: {e}"
                    )
                    self.log_error(
                        "metadata", f"{chain_name}:{contract_address}", str(e)
                    )
                return None

    def log_error(self, category, item_id, error):
        """Log general errors to a file for tracking."""
        log_file = os.path.join(self.logs_dir, f"{category}_errors.log")

        with open(log_file, "a") as f:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"{timestamp} | {item_id} | {error}\n")

    def clean_removed_tokens(self, chain_name, current_ids):
        """Remove tokens that are no longer in the list."""
        previous_data = self.get_previous_data(chain_name)
        previous_ids = [coin["id"] for coin in previous_data]
        removed_ids = [
            coin_id for coin_id in previous_ids if coin_id not in current_ids
        ]

        if removed_ids:
            print(
                f"Removing {len(removed_ids)} tokens no longer in the list for {chain_name}:"
            )
            for removed_id in removed_ids:
                png_path = os.path.join(
                    self.base_dir, chain_name, "pngs", f"{removed_id}.png"
                )
                if os.path.exists(png_path):
                    os.remove(png_path)
                    print(f"Removed image: {png_path}")

    def fetch_token_images(self, chain_name, coins):
        """Download images for new tokens."""
        previous_data = self.get_previous_data(chain_name)
        previous_ids = [coin["id"] for coin in previous_data]
        new_coins = [coin for coin in coins if coin["id"] not in previous_ids]
        print(f"Found {len(new_coins)} new tokens for {chain_name}")

        failed_downloads = []
        for coin in new_coins:
            if coin.get("image_url"):
                png_path = os.path.join(
                    self.base_dir, chain_name, "pngs", f"{coin['id']}.png"
                )
                success = self.download_image(coin["image_url"], png_path)
                if not success:
                    failed_downloads.append(coin["id"])

        if failed_downloads:
            print(
                f"Failed to download {len(failed_downloads)} images: {', '.join(failed_downloads)}"
            )

    def get_previous_data(self, chain_name):
        """Load the previous data for comparison."""
        data_path = os.path.join(self.base_dir, chain_name, "data.json")
        if os.path.exists(data_path):
            try:
                with open(data_path, "r") as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading previous data for {chain_name}: {e}")
        return []

    def download_image(self, image_url, save_path):
        """Download image from URL and save as PNG."""
        try:
            response = requests.get(image_url, stream=True)
            response.raise_for_status()
            with open(save_path, "wb") as out_file:
                shutil.copyfileobj(response.raw, out_file)
            print(f"Downloaded image to {save_path}")
            return True
        except Exception as e:
            print(f"Error downloading image {image_url}: {e}")
            self.log_failed_download(image_url, save_path, str(e))
            return False

    def log_failed_download(self, image_url, save_path, error):
        """Log failed image downloads to a file for tracking."""
        log_file = os.path.join(self.logs_dir, "failed_downloads.log")

        with open(log_file, "a") as f:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
            f.write(
                f"{timestamp} | {os.path.basename(save_path)} | {image_url} | {error}\n"
            )

    def finalise_and_clean_up_tokens(self, top_tokens):

        final_tokens = []
        for coin in top_tokens:
            print(f"Finalising data for {coin.get('name')} on {coin.get('chain')}")
            metadata = coin.get("metadata", {}).copy()
            metadata.pop("logo", None)
            coin_info = {
                "extract_time": start_time,
                "id": coin.get("id"),
                "symbol": coin.get("symbol"),
                "name": coin.get("name"),
                "contract_address": coin.get("contract_address"),
                "local_image": f"{coin.get('id')}.png",
                "metadata": metadata,
            }
            final_tokens.append(coin_info)
        return final_tokens


def main():
    try:
        global start_time
        start_time = time.time()
        print("Starting CoinGecko data aggregation with Alchemy enrichment...")
        aggregator = CoinAggregator()

        import sys

        if len(sys.argv) > 1:
            chain_arg = sys.argv[1]
            if chain_arg in aggregator.chains:
                print(f"Processing only {chain_arg} chain...")
                aggregator.process_single_chain(chain_arg)
            else:
                print(
                    f"Chain '{chain_arg}' not found. Available chains: {', '.join(aggregator.chains.keys())}"
                )
                return
        else:
            aggregator.process_chains()

        end_time = time.time()
        execution_time = end_time - start_time
        minutes, seconds = divmod(execution_time, 60)
        hours, minutes = divmod(minutes, 60)

        print("\n" + "=" * 50)
        print("DATA AGGREGATION SUMMARY")
        print("=" * 50)
        print(f"Total execution time: {int(hours)}h {int(minutes)}m {int(seconds)}s")
        print(f"Supported chains: {', '.join(aggregator.chains.keys())}")
        print(
            f"Chains with Alchemy enrichment: {', '.join(aggregator.alchemy_networks.keys())}"
        )
        print("Metadata has been successfully written to data.json files")
        if aggregator.alchemy_api_key:
            print("Alchemy metadata enrichment: Enabled")
        else:
            print("Alchemy metadata enrichment: Disabled (no API key)")
        print("=" * 50)
        print("Data aggregation completed successfully!")

    except Exception as e:
        print(f"Error during data aggregation: {e}")
        try:
            logs_dir = os.path.join("site/public/tokens/logs")
            os.makedirs(logs_dir, exist_ok=True)
            log_file = os.path.join(logs_dir, "error.log")

            with open(log_file, "a") as f:
                timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{timestamp} | ERROR: {str(e)}\n")
            print(f"Process failed. See {log_file} for details.")
        except Exception as log_error:
            print(f"Could not write to error log file: {log_error}")


if __name__ == "__main__":
    main()
