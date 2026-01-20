import os

if os.getenv("NO_NETWORK") == "1":
    import network_guard

    network_guard.apply_guard()
