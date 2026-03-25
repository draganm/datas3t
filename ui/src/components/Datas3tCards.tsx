import { For } from "solid-js";
import type { Datas3tInfo } from "../api/types";
import { formatBytes } from "./FormatBytes";
import DatarangeSizeChart from "./DatarangeSizeChart";

interface Props {
  datas3ts: Datas3tInfo[];
}

export default function Datas3tCards(props: Props) {
  return (
    <div class="space-y-6">
      <For each={props.datas3ts}>
        {(d) => (
          <div class="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
            <div class="px-6 py-4 border-b border-gray-100">
              <div class="flex items-center justify-between">
                <h2 class="text-lg font-semibold text-gray-900">{d.datas3t_name}</h2>
                <span class="text-sm text-gray-500 bg-gray-100 px-3 py-1 rounded-full">
                  {d.bucket_name}
                </span>
              </div>
            </div>

            <div class="grid grid-cols-3 divide-x divide-gray-100">
              <div class="px-6 py-4 text-center">
                <div class="text-2xl font-bold text-gray-900">
                  {d.total_datapoints.toLocaleString()}
                </div>
                <div class="text-xs text-gray-500 uppercase tracking-wider mt-1">
                  Datapoints
                </div>
              </div>
              <div class="px-6 py-4 text-center">
                <div class="text-2xl font-bold text-gray-900">
                  {d.datarange_count.toLocaleString()}
                </div>
                <div class="text-xs text-gray-500 uppercase tracking-wider mt-1">
                  Dataranges
                </div>
              </div>
              <div class="px-6 py-4 text-center">
                <div class="text-2xl font-bold text-gray-900">
                  {formatBytes(d.total_bytes)}
                </div>
                <div class="text-xs text-gray-500 uppercase tracking-wider mt-1">
                  Total Size
                </div>
              </div>
            </div>

            <div class="px-6 py-4 border-t border-gray-100">
              <DatarangeSizeChart
                datas3tName={d.datas3t_name}
                datarangeCount={d.datarange_count}
              />
            </div>
          </div>
        )}
      </For>
    </div>
  );
}
