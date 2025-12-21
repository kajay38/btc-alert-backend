[file name]: Procfile
[file content begin]
web: gunicorn main:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:$PORT --timeout 120 --keep-alive 5
worker: python -c "from analysis_worker import AnalysisWorker; import asyncio; asyncio.run(AnalysisWorker(['BTCUSD', 'ETHUSD']).start())"
[file content end]
