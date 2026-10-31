# Security Policy

## Reporting a problem

If you find a security problem in this repository, for example an example that
teaches an unsafe pattern or a file that exposes something it shouldn't,
please **don't open a public issue**. Instead:

- Use GitHub's **Report a vulnerability** button on the
  [Security tab](https://github.com/arsinghin/redis-zero-to-production-code/security), or
- Email **arsinghin@gmail.com** with "SECURITY" in the subject.

You'll get a reply within 7 days. Once it's fixed, you'll be credited in the
fix unless you'd rather not be.

**Problems in Redis itself** should go to the Redis project, not here:
see [Redis security](https://github.com/redis/redis/security/policy).

## About this code

These are teaching examples, written to run against a **throwaway local
Redis**:

- They use demo credentials such as `change-me-123`. Never reuse them.
- They connect without TLS and without a password by default.
- Some change server settings (`maxmemory`, ACL users) on the server they
  connect to.

Part 8 of the book covers what to change before running Redis in production:
network isolation, ACL users, TLS, blocked commands, and patching.

## Supported versions

Only the latest commit on `main` is maintained.
