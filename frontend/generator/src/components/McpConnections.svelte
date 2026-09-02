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
