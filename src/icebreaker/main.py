from ui.dashboard import render_dashboard

def main():
    metrics_service = None
    try:
        render_dashboard()
    except Exception as e:
        print(f"Application error: {e}")
    finally:
        if metrics_service:
            metrics_service.repo.close()  # Explicitly close the connection

if __name__ == "__main__":
    main()