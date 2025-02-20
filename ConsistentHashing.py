import hashlib
import bisect


class ConsistentHashing:
    def __init__(self, replicas=3):
        self.replicas = replicas  # Virtual nodes to balance load
        self.ring = {}  # Hash ring
        self.sorted_keys = []  # Sorted list of hashes

    def _hash(self, key):
        """Generate a hash for a given key."""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_server(self, server):
        """Add a server (with replicas) to the hash ring."""
        for i in range(self.replicas):
            server_hash = self._hash(f"{server}-{i}")
            self.ring[server_hash] = server
            bisect.insort(self.sorted_keys, server_hash)
        print(f"Added server: {server}")

    def remove_server(self, server):
        """Remove a server (and its replicas) from the hash ring."""
        for i in range(self.replicas):
            server_hash = self._hash(f"{server}-{i}")
            if server_hash in self.ring:
                self.ring.pop(server_hash)
                self.sorted_keys.remove(server_hash)
        print(f"Removed server: {server}")

    def get_server(self, key):
        """Find the appropriate server for a given key."""
        if not self.ring:
            return None
        key_hash = self._hash(key)
        index = bisect.bisect(self.sorted_keys, key_hash) % len(self.sorted_keys)
        return self.ring[self.sorted_keys[index]]

    def display_ring(self):
        """Display the current hash ring."""
        print("\nCurrent Hash Ring:")
        for key in self.sorted_keys:
            print(f"{key} -> {self.ring[key]}")


# Testing the Consistent Hashing implementation
if __name__ == "__main__":
    ch = ConsistentHashing()

    # Step 1: Add servers
    ch.add_server("ServerA")
    ch.add_server("ServerB")
    ch.add_server("ServerC")

    ch.display_ring()

    # Step 2: Assign keys to servers
    keys = ["User1", "User2", "User3", "User4", "User5"]
    for key in keys:
        print(f"{key} is stored in {ch.get_server(key)}")

    # Step 3: Remove a server and check reassignment
    print("\nRemoving ServerB...")
    ch.remove_server("ServerB")

    ch.display_ring()

    for key in keys:
        print(f"{key} is now stored in {ch.get_server(key)}")
