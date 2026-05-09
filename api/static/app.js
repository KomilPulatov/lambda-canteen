const day = new Date().toISOString().slice(0, 10);
document.getElementById("day").textContent = "(" + day + ")";

let revenueChart = null;
let topItemsChart = null;
let ordersPerHourChart = null;

async function refresh() {
  const [revRes, topRes, hourRes] = await Promise.all([
    fetch("/metrics/revenue-by-category?day=" + day),
    fetch("/metrics/top-items?day=" + day + "&limit=5"),
    fetch("/metrics/orders-per-hour?day=" + day),
  ]);

  const rev = await revRes.json();
  const top = await topRes.json();
  const hourly = await hourRes.json();

  const revLabels = rev.data.map(x => x.category);
  const revValues = rev.data.map(x => x.revenue);

  if (revenueChart) revenueChart.destroy();

  revenueChart = new Chart(
    document.getElementById("revenueChart"),
    {
      type: "bar",
      data: {
        labels: revLabels,
        datasets: [
          {
            label: "Revenue (UZS)",
            data: revValues,
          }
        ],
      },
      options: {
        animation: false,
      },
    }
  );

  const topLabels = top.data.map(x => x.item_id);
  const topValues = top.data.map(x => x.quantity);

  if (topItemsChart) topItemsChart.destroy();

  topItemsChart = new Chart(
    document.getElementById("topItemsChart"),
    {
      type: "bar",
      data: {
        labels: topLabels,
        datasets: [
          {
            label: "Quantity sold",
            data: topValues,
          }
        ],
      },
      options: {
        animation: false,
        indexAxis: "y",
      },
    }
  );

  const hourLabels = hourly.data.map(x => x.hour);
  const hourValues = hourly.data.map(x => x.orders);

  if (ordersPerHourChart) ordersPerHourChart.destroy();

  ordersPerHourChart = new Chart(
    document.getElementById("ordersPerHourChart"),
    {
      type: "line",
      data: {
        labels: hourLabels,
        datasets: [
          {
            label: "Orders per hour",
            data: hourValues,
          }
        ],
      },
      options: {
        animation: false,
      },
    }
  );
}

refresh();
setInterval(refresh, 5000);