import { createResource, Show } from "solid-js";
import { fetchDatas3ts } from "../api/client";
import Datas3tTable from "./Datas3tTable";
import EmptyState from "./EmptyState";

export default function Dashboard() {
  const [datas3ts] = createResource(fetchDatas3ts);

  return (
    <>
      <Show when={datas3ts.loading}>
        <div class="text-center py-8 text-gray-500">Loading...</div>
      </Show>
      <Show when={datas3ts.error}>
        <div class="text-center py-8 text-red-500">
          Failed to load datas3ts: {datas3ts.error?.message}
        </div>
      </Show>
      <Show when={datas3ts()}>
        {(data) => (
          <Show when={data().length > 0} fallback={<EmptyState />}>
            <Datas3tTable datas3ts={data()} />
          </Show>
        )}
      </Show>
    </>
  );
}
