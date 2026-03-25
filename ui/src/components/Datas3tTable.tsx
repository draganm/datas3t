import { For } from "solid-js";
import type { Datas3tInfo } from "../api/types";
import { formatBytes } from "./FormatBytes";
import DatarangeSizeChart from "./DatarangeSizeChart";

interface Props {
  datas3ts: Datas3tInfo[];
}

export default function Datas3tTable(props: Props) {
  return (
    <div class="overflow-x-auto">
      <table class="min-w-full bg-white rounded-lg shadow-md">
        <thead class="bg-gray-100 border-b">
          <tr>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
              Name
            </th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
              Bucket
            </th>
            <th class="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
              Datapoints
            </th>
            <th class="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
              Dataranges
            </th>
            <th class="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
              Total Size
            </th>
            <th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
              Datarange Sizes
            </th>
          </tr>
        </thead>
        <tbody class="divide-y divide-gray-200">
          <For each={props.datas3ts}>
            {(d) => (
              <tr class="hover:bg-gray-50">
                <td class="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                  {d.datas3t_name}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                  {d.bucket_name}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500 text-right">
                  {d.total_datapoints.toLocaleString()}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500 text-right">
                  {d.datarange_count.toLocaleString()}
                </td>
                <td class="px-6 py-4 whitespace-nowrap text-sm text-gray-500 text-right">
                  {formatBytes(d.total_bytes)}
                </td>
                <td class="px-6 py-4">
                  <DatarangeSizeChart
                    datas3tName={d.datas3t_name}
                    datarangeCount={d.datarange_count}
                  />
                </td>
              </tr>
            )}
          </For>
        </tbody>
      </table>
    </div>
  );
}
