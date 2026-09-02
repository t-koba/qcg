<script lang="ts">
  import type { SchemaIssue } from "./field";

  let {
    id,
    issues,
    pathForIssue,
  }: {
    id: string;
    issues: SchemaIssue[];
    pathForIssue: (issue: SchemaIssue) => string;
  } = $props();
</script>

<ul id={id} class="schema-errors" role="alert">
  {#each issues as issue}<li>{pathForIssue(issue)}: {issue.message}</li>{/each}
</ul>
