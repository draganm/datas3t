import { createResource, Show, onCleanup } from "solid-js";
import uPlot from "uplot";
import "uplot/dist/uPlot.min.css";
import { fetchDataranges } from "../api/client";
import { formatBytes } from "./FormatBytes";

interface Props {
  datas3tName: string;
  datarangeCount: number;
}

export default function DatarangeSizeChart(props: Props) {
  let chart: uPlot | undefined;

  const [dataranges] = createResource(
    () => props.datas3tName,
    (name) => fetchDataranges(name),
  );

  onCleanup(() => {
    chart?.destroy();
  });

  function initChart(
    container: HTMLDivElement,
    ranges: { min_datapoint_key: number; max_datapoint_key: number; size_bytes: number }[],
  ) {
    if (chart) chart.destroy();
    if (ranges.length === 0) return;

    const xData = ranges.map((_, i) => i);
    const yData = ranges.map((r) => r.size_bytes);

    const opts: uPlot.Options = {
      width: 400,
      height: 150,
      cursor: { show: false },
      legend: { show: false },
      axes: [
        { show: false },
        {
          size: 60,
          values: (_u: uPlot, ticks: number[]) => ticks.map((v) => formatBytes(v)),
        },
      ],
      series: [
        {},
        {
          fill: "rgba(59, 130, 246, 0.5)",
          stroke: "rgba(59, 130, 246, 1)",
          paths: uPlot.paths.bars!({ size: [0.6, 100] }),
        },
      ],
    };

    chart = new uPlot(opts, [xData, yData], container);
  }

  return (
    <Show
      when={!dataranges.loading && dataranges()}
      fallback={
        props.datarangeCount > 0 ? (
          <span class="text-sm text-gray-400">Loading chart...</span>
        ) : (
          <span class="text-sm text-gray-400">No dataranges</span>
        )
      }
    >
      {(resp) => {
        const ranges = resp().dataranges;
        if (!ranges || ranges.length === 0) {
          return <span class="text-sm text-gray-400">No dataranges</span>;
        }
        return (
          <div
            ref={(el) => {
              requestAnimationFrame(() => initChart(el, ranges));
            }}
            style={{ height: "150px", width: "400px" }}
          />
        );
      }}
    </Show>
  );
}
