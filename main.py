from experiments import run_all_experiments, print_results_table, print_summary, save_metric_plots, print_combined_stress_bandwidth_winners

def main():
    results = run_all_experiments(trials=10)
    print_results_table(results)
    print_summary(results)
    print_combined_stress_bandwidth_winners(results)
    plots_dir = save_metric_plots(results)
    print(f"\nPlots saved to: {plots_dir.resolve()}")

if __name__ == "__main__":
    main()
