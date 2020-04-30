

1. Expose connection handle in watch, so stream.stop() works
2. Reconsider exception handling on _stop in watch (original probably ok), handle in informer instead
3. Informer should pass a high timeout_seconds (server) and low _request_timeout (client). Verify _request_timeout is an Idle timeout.
4. When informer is relisting, do a diff of initial list vs cache, and fire added/modified/deleted events.
5. Write integration (in-process) or e2e tests highlighting client timeout handling.

