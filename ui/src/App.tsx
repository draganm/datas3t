import { createResource, createSignal, Show } from "solid-js";
import { fetchDatas3ts } from "./api/client";
import Dashboard from "./components/Dashboard";
import CreateDatas3tModal from "./components/CreateDatas3tModal";

export default function App() {
  const [datas3ts, { refetch }] = createResource(fetchDatas3ts);
  const [showCreateModal, setShowCreateModal] = createSignal(false);

  return (
    <div class="min-h-screen bg-gray-50">
      <header class="bg-white border-b border-gray-200">
        <div class="max-w-5xl mx-auto px-6 py-4 flex items-center justify-between">
          <h1 class="text-2xl font-bold text-gray-900">Datas3t</h1>
          <button
            class="bg-blue-600 text-white px-4 py-2 rounded-lg text-sm font-medium hover:bg-blue-700 transition-colors"
            onClick={() => setShowCreateModal(true)}
          >
            Create Datas3t
          </button>
        </div>
      </header>
      <main class="max-w-5xl mx-auto px-6 py-8">
        <Dashboard datas3ts={datas3ts} />
      </main>
      <Show when={showCreateModal()}>
        <CreateDatas3tModal
          onClose={() => setShowCreateModal(false)}
          onCreated={() => {
            setShowCreateModal(false);
            refetch();
          }}
        />
      </Show>
    </div>
  );
}
