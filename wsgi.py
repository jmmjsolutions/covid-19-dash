"""Application entry point."""
from application import create_app

app = create_app()

def main():
    app.run(host='0.0.0.0', port=5100, debug=True)


if __name__ == "__main__":
    main()