from pathlib import Path
from web3 import Web3, middleware
from web3.gas_strategies.time_based import fast_gas_price_strategy
from conf.base import BASE_MAINNET, CONTRACT_ADDRESS


class Contract:
    def __init__(self, contract_address=CONTRACT_ADDRESS, base_mainnet=BASE_MAINNET, private_key=None):
        self.w3 = Web3(Web3.HTTPProvider(base_mainnet))
        self.w3.eth.set_gas_price_strategy(fast_gas_price_strategy)
        self.w3.middleware_onion.add(middleware.time_based_cache_middleware)
        self.w3.middleware_onion.add(middleware.latest_block_based_cache_middleware)
        self.w3.middleware_onion.add(middleware.simple_cache_middleware)

        self.private_key = private_key
        self.account = self.w3.eth.account.from_key(self.private_key) if self.private_key else None
        script_location = Path(__file__).absolute().parent
        abi_path = script_location / "../abi/friendTechAbi.json"
        with open(abi_path, "r") as file:
            self.abi = file.read()

        self.contract = self.w3.eth.contract(address=contract_address, abi=self.abi)

    def _get_valid_checksum_address(self, address):
        """Validate and convert the provided address to a checksum address"""
        if not self.w3.is_address(address):
            raise ValueError(f"Invalid Ethereum address: {address}")
        return self.w3.to_checksum_address(address)
    
    def _execute_contract_function(self, function_name, **kwargs):
        """Generic method to execute a specified contract function"""
        try:
            for key in ['address', 'subject_address']:
                if key in kwargs:
                    kwargs[key] = self._get_valid_checksum_address(kwargs[key])

            contract_function = getattr(self.contract.functions, function_name)
            return contract_function(**kwargs).call()
        except Exception as e:
            print(f"An error occurred while executing {function_name}: {e}")
            return None  #TODO Consider appropriate error handling
    
    def _create_signed_transaction(self, method, value=None):
        """Create a signed transaction for a specified contract function"""
        try:
            # Estimate the gas for the transaction
            estimated_gas = method.estimate_gas({'from': self.account.address, 'value': value})
            
            # Prepare the basic transaction structure
            transaction_details = {
                'chainId': self.w3.eth.chain_id,
                'gas': int(estimated_gas * 1.1),  # Adding a 10% buffer over the estimated gas
                'gasPrice': self.w3.eth.generate_gas_price(),
                'nonce': self.w3.eth.get_transaction_count(self.account.address),
            }

            # Include value only if it's specified (i.e., not None and more than 0)
            if value:
                transaction_details['value'] = value

            # Building a transaction
            built_transaction = method.build_transaction(transaction_details)

            # Signing the transaction
            signed_txn = self.w3.eth.account.sign_transaction(built_transaction, private_key=self.private_key)
            return signed_txn
        except Exception as e:
            print(f"An error occurred while creating the transaction: {e}")
            return None
    
    def calc_price(self, supply, amount):
        """Calculate the price based on supply and amount"""
        try:
            sum1 = 0 if supply == 0 else (supply - 1) * supply * (2 * (supply - 1) + 1) // 6
            sum2 = 0 if supply == 0 and amount == 1 else (supply - 1 + amount) * (supply + amount) * (2 * (supply - 1 + amount) + 1) // 6
            summation = sum2 - sum1
            return summation * 10**18 // 16000
        except Exception as e:
            print(f"An error occurred while calculating the price: {e}")
            return None
        
    def calc_buy_price_after_fee(self, supply, amount):
        """Calculate the buy price after including all applicable fees"""
        try:
            price = self.calc_price(supply=supply, amount=amount)
            if price is None:
                raise ValueError("Error calculating the base price.")
            
            protocol_fee = price * 0.05
            subject_fee = price * 0.05
            return price + protocol_fee + subject_fee
            
        except Exception as e:
            print(f"An error occurred while calculating the buy price after fees: {e}")
            return None
        
    def calc_sell_price_after_fee(self, supply, amount):
        """Calculate the sell price after including all applicable fees"""
        try:
            price = self.calc_price(supply=supply - amount, amount=amount)
            if price is None:
                raise ValueError("Error calculating the base price.")
            
            protocol_fee = price * 0.05
            subject_fee = price * 0.05
            return price - protocol_fee - subject_fee
            
        except Exception as e:
            print(f"An error occurred while calculating the buy price after fees: {e}")
            return None

    def get_buy_price(self, address, amount):
        """Returns buy price of a share in wei"""
        return self._execute_contract_function('getBuyPrice', address=address, amount=amount)

    def get_buy_price_after_fee(self, address, amount):
        """Returns buy price of a share after fees in wei"""
        return self._execute_contract_function('getBuyPriceAfterFee', address=address, amount=amount)
            
    def get_sell_price(self, address, amount):
        """Returns sell price of a share in wei"""
        return self._execute_contract_function('getSellPrice', address=address, amount=amount)

    def get_sell_price_after_fee(self, address, amount):
        """Returns sell price of a share after fees in wei"""
        return self._execute_contract_function('getSellPriceAfterFee', address=address, amount=amount)

    def get_shares_supply(self, address):
        """Returns supply of share"""
        return self._execute_contract_function('sharesSupply', address=address)

    def get_shares_owned(self, address, subject_address):
        """Returns how many shares of address the subject_address owns"""
        return self._execute_contract_function('sharesBalance', address=address, subject_address=subject_address)
    
    def buy_shares(self, address, amount=1):
        """Buys shares of a given address"""
        try:
            if self.account is None:
                raise ValueError("Private key is required to execute this operation.")
            
            # Ensure the address is valid
            address = self._get_valid_checksum_address(address)

            # Attempt to calculate the cost with the fee included locally
            price = self.calc_buy_price_after_fee(address, amount)
            if price is None:
                print("Local price calculation failed, retrieving price from the contract.")
                price = self.get_buy_price_after_fee(address, amount)
                if price is None:
                    print("Could not retrieve the buy price after fee from the contract.")
                    return
            if price == 0:
                print("Only the shares' subject can buy the first share.")
                return
            
            # Check if funds are sufficient
            if self.w3.eth.get_balance(self.account.address) < price:
                print("Insufficient funds for this transaction.")
                return

            contract_function = self.contract.functions.buyShares(address, amount)
            signed_txn = self._create_signed_transaction(contract_function, value=price)
            tx_hash = self.w3.eth.send_raw_transaction(signed_txn.rawTransaction)
            basescan_link = f"https://basescan.org/tx/{tx_hash.hex()}"
            print("Transaction sent:\n" + basescan_link)
            return basescan_link
        except Exception as e:
            print(f"An error occurred while buying shares: {e}")
    
    def sell_shares(self, address, amount=1):
        """Sells shares of a given address"""
        try:
            if self.account is None:
                raise ValueError("Private key is required to execute this operation.")
            
            # Ensure the address is valid
            address = self._get_valid_checksum_address(address)
            
            # Check the number of shares the user owns
            owned_shares = self.get_shares_owned(address, self.account.address)
            if owned_shares is None:
                print("Could not retrieve the number of shares owned.")
                return
            if owned_shares < amount:
                print(f"You own {owned_shares} shares but tried to sell {amount}.")
                return
            
            # Check the total supply of shares to ensure we're not selling the last share
            shares_supply = self.get_shares_supply(address)
            if shares_supply <= amount:
                print("Cannot sell the last share.")
                return
        
            contract_function = self.contract.functions.sellShares(address, amount)
            signed_txn = self._create_signed_transaction(contract_function)
            tx_hash = self.w3.eth.send_raw_transaction(signed_txn.rawTransaction)
            basescan_link = f"https://basescan.org/tx/{tx_hash.hex()}"
            print("Transaction sent:\n" + basescan_link)
            return basescan_link
        except Exception as e:
            print(f"An error occurred while selling shares: {e}")