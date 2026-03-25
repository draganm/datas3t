import { createResource, createSignal, For, Show } from "solid-js";
import { fetchBuckets, createDatas3t } from "../api/client";

interface Props {
  onClose: () => void;
  onCreated: () => void;
}

export default function CreateDatas3tModal(props: Props) {
  const [name, setName] = createSignal("");
  const [selectedBucket, setSelectedBucket] = createSignal("");
  const [error, setError] = createSignal("");
  const [submitting, setSubmitting] = createSignal(false);
  const [buckets] = createResource(fetchBuckets);

  const nameValid = () => name() === "" || /^[a-zA-Z0-9_-]+$/.test(name());

  async function handleSubmit(e: Event) {
    e.preventDefault();
    setError("");

    if (!name() || !/^[a-zA-Z0-9_-]+$/.test(name())) {
      setError("Name must contain only letters, numbers, hyphens, and underscores.");
      return;
    }
    if (!selectedBucket()) {
      setError("Please select a bucket.");
      return;
    }

    setSubmitting(true);
    try {
      await createDatas3t(name(), selectedBucket());
      props.onCreated();
    } catch (err: any) {
      setError(err.message || "Failed to create datas3t.");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div
      class="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
      onClick={(e) => {
        if (e.target === e.currentTarget) props.onClose();
      }}
    >
      <div class="bg-white rounded-xl shadow-xl w-full max-w-md mx-4 p-6">
        <h2 class="text-xl font-semibold text-gray-900 mb-6">Create Datas3t</h2>

        <form onSubmit={handleSubmit} class="space-y-4">
          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">Name</label>
            <input
              type="text"
              value={name()}
              onInput={(e) => setName(e.currentTarget.value)}
              placeholder="my-dataset"
              class="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
              autofocus
            />
            <Show when={!nameValid()}>
              <p class="text-xs text-red-500 mt-1">
                Only letters, numbers, hyphens, and underscores allowed.
              </p>
            </Show>
          </div>

          <div>
            <label class="block text-sm font-medium text-gray-700 mb-1">Bucket</label>
            <Show when={buckets.loading}>
              <p class="text-sm text-gray-400">Loading buckets...</p>
            </Show>
            <Show when={buckets.error}>
              <p class="text-sm text-red-500">Failed to load buckets.</p>
            </Show>
            <Show when={buckets()}>
              {(list) => (
                <Show
                  when={list().length > 0}
                  fallback={
                    <p class="text-sm text-gray-500">No buckets configured. Add a bucket first.</p>
                  }
                >
                  <select
                    value={selectedBucket()}
                    onChange={(e) => setSelectedBucket(e.currentTarget.value)}
                    class="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none"
                  >
                    <option value="">Select a bucket...</option>
                    <For each={list()}>
                      {(b) => (
                        <option value={b.name}>
                          {b.name} ({b.endpoint} / {b.bucket})
                        </option>
                      )}
                    </For>
                  </select>
                </Show>
              )}
            </Show>
          </div>

          <Show when={error()}>
            <p class="text-sm text-red-600 bg-red-50 border border-red-200 rounded-lg px-3 py-2">
              {error()}
            </p>
          </Show>

          <div class="flex justify-end gap-3 pt-2">
            <button
              type="button"
              onClick={props.onClose}
              class="px-4 py-2 text-sm text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={submitting() || !name() || !nameValid() || !selectedBucket()}
              class="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {submitting() ? "Creating..." : "Create"}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
