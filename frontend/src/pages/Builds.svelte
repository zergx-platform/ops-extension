<script lang="ts">
  import { onMount } from 'svelte'
  import { api, type Template } from '$lib/api'
  import Page from '$lib/components/Page.svelte'
  import * as Card from '$lib/components/ui/card'
  import * as Button from '$lib/components/ui/button'
  import * as Input from '$lib/components/ui/input'
  import * as Select from '$lib/components/ui/native-select'
  import * as Badge from '$lib/components/ui/badge'
  import { Hammer } from '@lucide/svelte'

  // ---- mode: repo (org/repo/bookmark) or raw (paste a Containerfile) ----
  let mode = $state<'repo' | 'raw'>('repo')
  let org = $state('')
  let repo = $state('')
  let session = $state('')
  let bookmark = $state('main')
  let tag = $state('')
  let dockerfile = $state('Dockerfile')
  let rawContent = $state('FROM alpine:3.20\n\nCMD ["sh", "-c", "echo hello"]\n')
  let building = $state(false)
  let result = $state('')

  let images: string[] = $state([])
  let templates: Template[] = $state([])

  async function build() {
    building = true
    result = ''
    try {
      let r: { ok: boolean; image?: string; error?: string }
      if (mode === 'repo') {
        const b: Record<string, string> = { tag, dockerfile }
        if (session) b.session = session
        else {
          b.org = org
          b.repo = repo
          b.bookmark = bookmark
        }
        r = await api.buildImage({
          org: b.org ?? '',
          repo: b.repo ?? '',
          bookmark: b.bookmark ?? 'main',
          tag,
          dockerfile,
        })
      } else {
        r = await api.buildRaw({ dockerfile: rawContent, tag })
      }
      result = r.ok ? `built ${r.image}` : (r.error ?? 'failed')
      images = (await api.images()).repositories
    } catch (e) {
      result = String(e)
    } finally {
      building = false
    }
  }

  function loadTemplate(t: Template) {
    rawContent = t.content
    mode = 'raw'
  }

  onMount(async () => {
    try {
      images = (await api.images()).repositories
      templates = (await api.templates()).templates
    } catch {}
  })
</script>

<Page title="Builds" desc="Build container images (from a repo or a pasted Containerfile) via buildkitd">
  <div class="grid gap-4 lg:grid-cols-3">
    <Card.Root class="lg:col-span-2">
      <Card.Header class="flex-row items-center justify-between">
        <Card.Title class="text-sm">Build image</Card.Title>
        <div class="flex rounded-md border">
          <button
            class="px-3 py-1 text-xs {mode === 'repo' ? 'bg-primary text-primary-foreground' : 'text-muted-foreground'}"
            onclick={() => (mode = 'repo')}
          >From repo</button
          >
          <button
            class="px-3 py-1 text-xs border-l {mode === 'raw' ? 'bg-primary text-primary-foreground' : 'text-muted-foreground'}"
            onclick={() => (mode = 'raw')}
          >Paste Containerfile</button
          >
        </div>
      </Card.Header>
      <Card.Content class="space-y-3">
        {#if mode === 'repo'}
          <Input.Root placeholder="session (org:repo:bookmark) — or fill below" bind:value={session} />
          <div class="grid grid-cols-2 gap-2">
            <Input.Root placeholder="org" bind:value={org} />
            <Input.Root placeholder="repo" bind:value={repo} />
            <Input.Root placeholder="bookmark (main)" bind:value={bookmark} />
            <Input.Root placeholder="tag" bind:value={tag} />
          </div>
          <Input.Root placeholder="dockerfile path" bind:value={dockerfile} />
        {:else}
          <Input.Root placeholder="tag" bind:value={tag} />
          <textarea
            class="h-64 w-full rounded-md bg-muted p-3 font-mono text-xs"
            placeholder="FROM alpine:3.20 ..."
            bind:value={rawContent}
          ></textarea>
        {/if}
        <div class="flex items-center gap-2">
          <Button.Root onclick={build} disabled={building || !tag || (mode === 'repo' && !session && (!org || !repo))}>
            <Hammer class="size-4" /> {building ? 'Building…' : 'Build + push'}
          </Button.Root>
        </div>
        {#if result}<pre class="rounded-md bg-muted p-3 font-mono text-xs whitespace-pre-wrap">{result}</pre>{/if}
      </Card.Content>
    </Card.Root>

    <Card.Root>
      <Card.Header><Card.Title class="text-sm">Containerfile templates</Card.Title></Card.Header>
      <Card.Content class="space-y-2">
        {#each templates as t (t.name)}
          <div class="flex items-center justify-between rounded-md border px-3 py-2">
            <code class="font-mono text-xs">{t.name}</code>
            <Button.Root size="sm" variant="outline" onclick={() => loadTemplate(t)}>use</Button.Root>
          </div>
        {/each}
      </Card.Content>
    </Card.Root>
  </div>

  <Card.Root class="mt-4">
    <Card.Header><Card.Title class="text-sm">Registry images ({images.length})</Card.Title></Card.Header>
    <Card.Content>
      <div class="flex flex-wrap gap-1.5">
        {#each images as img (img)}
          <code class="rounded bg-muted px-1.5 py-0.5 font-mono text-xs">{img}</code>
        {/each}
      </div>
    </Card.Content>
  </Card.Root>
</Page>
