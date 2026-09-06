<script lang="ts">
  import { onDestroy, onMount } from "svelte";
  import { ApiClient } from "../api/client";
  import { McpConnectionController, type McpSnapshot } from "../mcp-authorization";
  import type { Messages } from "../messages";

  let { messages }: { messages: Messages } = $props();
  const controller = new McpConnectionController(new ApiClient());
  let snapshot = $state<McpSnapshot>(controller.snapshot);
  let unsubscribe: (() => void) | null = null;

  onMount(() => {
    unsubscribe = controller.subscribe((next) => { snapshot = next; });
    void controller.initialize();
  });

  onDestroy(() => {
    unsubscribe?.();
    controller.destroy();
  });

  let hasContent = $derived(snapshot.phase === "loading" || snapshot.servers.length > 0 || Boolean(snapshot.errorText));

  function noticeText(): string {
    switch (snapshot.notice) {
      case "popup_blocked": return messages.mcpPopupBlocked;
      case "canceled": return messages.mcpAuthorizationCanceled;
      case "timeout": return messages.mcpAuthorizationTimeout;
      case "disconnected": return messages.mcpDisconnected;
      default: return "";
    }
  }

  function serverDetails(transport: string, auth: string): string {
    return [transport, auth].filter(Boolean).join(" · ");
  }
</script>

{#if hasContent}
  <section class="mcp-connections" aria-labelledby="mcp-connections-title">
    <div class="mcp-heading">
      <p id="mcp-connections-title" class="nav-label">{messages.mcpConnections}</p>
      {#if snapshot.phase === "loading"}<span class="mcp-loading">{messages.mcpChecking}</span>{/if}
    </div>

    {#if snapshot.servers.length > 0}
      <div class="mcp-server-list">
        {#each snapshot.servers as server (server.id)}
          <div class="mcp-server-row">
            <div class="mcp-server-copy">
              <strong>{server.id}</strong>
              <small>{serverDetails(server.transport, server.auth)}</small>
            </div>
            {#if snapshot.disconnectingServerId === server.id}
              <span class="mcp-status pending">{messages.mcpDisconnecting}</span>
            {:else if server.authorized}
              <div class="mcp-server-action">
                <span class="mcp-status connected">{messages.mcpConnected}</span>
                {#if server.auth === "oauth"}
                  <button
                    class="mcp-disconnect"
                    type="button"
                    disabled={snapshot.phase !== "idle"}
                    onclick={() => void controller.disconnect(server.id)}
                  >{messages.mcpDisconnect}</button>
                {/if}
              </div>
            {:else if snapshot.authorizingServerId === server.id}
              <div class="mcp-server-action">
                <span class="mcp-status pending">{snapshot.phase === "canceling" ? messages.mcpCanceling : messages.mcpConnecting}</span>
                {#if snapshot.phase !== "canceling"}
                  <button class="mcp-cancel" type="button" onclick={() => void controller.cancelAuthorization()}>{messages.mcpCancel}</button>
                {/if}
              </div>
            {:else}
              <button
                class="mcp-connect"
                type="button"
                disabled={snapshot.phase !== "idle"}
                onclick={() => void controller.authorize(server.id)}
              >{messages.mcpConnect}</button>
            {/if}
          </div>
        {/each}
      </div>
    {/if}

    {#if noticeText()}<p class="mcp-notice" role="status">{noticeText()}</p>{/if}
    {#if snapshot.errorText}<p class="mcp-error" role="alert">{snapshot.errorText}</p>{/if}
  </section>
{/if}

<style>
  /* Connection list scoped to the sidebar footer. */
  .mcp-connections {
    border-top: 1px solid rgba(152, 162, 179, .2);
    margin-top: auto;
    padding: 20px 8px 0;
  }

  .mcp-heading {
    align-items: center;
    display: flex;
    justify-content: space-between;
    min-height: 18px;
  }

  .mcp-heading .nav-label {
    margin: 0;
  }

  .mcp-loading {
    color: var(--sidebar-faint);
    font-size: 10px;
  }

  .mcp-server-list {
    display: grid;
    gap: var(--space-sm);
    margin-top: 10px;
  }

  .mcp-server-row {
    align-items: center;
    display: flex;
    gap: var(--space-sm);
    justify-content: space-between;
    min-width: 0;
  }

  .mcp-server-copy {
    display: grid;
    gap: 1px;
    min-width: 0;
  }

  .mcp-server-copy strong {
    color: var(--sidebar-bright);
    font-size: 12px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .mcp-server-copy small {
    color: var(--sidebar-faint);
    font-size: 10px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .mcp-connect,
  .mcp-cancel,
  .mcp-disconnect {
    background: transparent;
    border: 1px solid rgba(152, 162, 179, .35);
    border-radius: var(--radius-sm);
    color: var(--sidebar-fg);
    cursor: pointer;
    flex: 0 0 auto;
    font-size: 10px;
    font-weight: 700;
    padding: 4px 7px;
  }

  .mcp-connect:hover:not(:disabled),
  .mcp-cancel:hover:not(:disabled),
  .mcp-disconnect:hover:not(:disabled) {
    background: rgba(255, 255, 255, .08);
    border-color: var(--sidebar-faint);
    color: #fff;
  }

  .mcp-disconnect {
    color: #fda4af;
  }

  .mcp-disconnect:hover:not(:disabled) {
    border-color: #fda4af;
    color: #fff;
  }

  .mcp-server-action {
    align-items: center;
    display: flex;
    flex: 0 0 auto;
    gap: var(--space-xs);
  }

  .mcp-status {
    font-size: 10px;
    font-weight: 700;
    white-space: nowrap;
  }

  .mcp-status.connected { color: #6ce0b1; }
  .mcp-status.pending { color: #9fe3c2; }

  .mcp-notice,
  .mcp-error {
    font-size: 11px;
    margin: 10px 0 0;
  }

  .mcp-notice { color: #9fe3c2; }
  .mcp-error { color: #fda4af; }

  @media (max-width: 820px) {
    .mcp-connections {
      margin-top: 16px;
      padding: 14px 4px 0;
    }

    .mcp-server-list {
      display: flex;
      gap: 14px;
      overflow-x: auto;
    }

    .mcp-server-row {
      flex: 0 0 auto;
      min-width: 190px;
    }
  }
</style>
