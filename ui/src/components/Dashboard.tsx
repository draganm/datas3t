import { Show, type Resource } from "solid-js";
import type { Datas3tInfo } from "../api/types";
import Datas3tCards from "./Datas3tCards";
import EmptyState from "./EmptyState";

interface Props {
  datas3ts: Resource<Datas3tInfo[]>;
}

export default function Dashboard(props: Props) {
  return (
    <>
      <Show when={props.datas3ts.loading}>
        <div class="text-center py-8 text-gray-500">Loading...</div>
      </Show>
      <Show when={props.datas3ts.error}>
        <div class="text-center py-8 text-red-500">
          Failed to load datas3ts: {props.datas3ts.error?.message}
        </div>
      </Show>
      <Show when={props.datas3ts()}>
        {(data) => (
          <Show when={data().length > 0} fallback={<EmptyState />}>
            <Datas3tCards datas3ts={data()} />
          </Show>
        )}
      </Show>
    </>
  );
}
