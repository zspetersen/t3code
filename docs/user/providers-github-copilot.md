# GitHub Copilot

GitHub Copilot support is experimental and off by default. Install
[GitHub Copilot CLI](https://docs.github.com/en/copilot/how-tos/copilot-cli/set-up-copilot-cli/install-copilot-cli)
on the machine running your environment, then authenticate there:

```sh
npm install -g @github/copilot
copilot login
```

For a remote or headless environment, use `copilot login --device-code` and finish
sign-in in your browser. See GitHub's
[authentication guide](https://docs.github.com/en/copilot/how-tos/copilot-cli/set-up-copilot-cli/authenticate-copilot-cli).

In the web or desktop app, open **Settings > Providers**, select that environment,
and enable **GitHub Copilot**. Set **Binary path** if `copilot` is not on the
server's `PATH`. Refresh provider status, then select a Copilot model in a thread.
Connected mobile clients use the same provider and account. Disable it again in
provider settings when you finish experimenting.

**COPILOT_HOME path** changes the CLI's configuration directory. It does not
guarantee a separate account: Copilot may share credentials through the system
keychain. If you customize this path, use the same `COPILOT_HOME` when signing in.
`COPILOT_GITHUB_TOKEN`, `GH_TOKEN`, and `GITHUB_TOKEN` environment variables take
precedence over stored login credentials, in that order.

## Experimental limits

Models come from your installed CLI and account. After changing either, refresh
provider status in Settings; on mobile, use **Refresh models** in thread settings.
Update the CLI on the environment's machine if compatibility problems occur.

Copilot session rollback is unavailable. Automatic title, branch name, and commit
message generation are unavailable through this provider; choose another provider
for those tasks. Questions appear as conversation text rather than structured
question forms.
