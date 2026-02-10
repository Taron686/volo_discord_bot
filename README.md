
# V.O.L.O Discord Transcription Bot

This project is a Discord bot that transcribes voice channel audio into text in real-time. It uses Whisper for audio transcription and is capable of handling multiple users in a voice channel.

## Features

- This project uses Pycord (see [Pycord Github](https://github.com/Pycord-Development/pycord))
- This project uses Faster Whisper (see [Faster Whisper Github](https://github.com/SYSTRAN/faster-whisper))
- Transcribes voice channel audio to text.
- Supports multiple users.
- Thread-safe operations for concurrent transcriptions.

## Setup

To set up and run this Discord bot, follow these steps:

### Prerequisites

- Python 3.7 or higher.
- Discord bot token (see [Discord Developer Portal](https://discord.com/developers/applications)).
- `ffmpeg` installed and added to your system's PATH.

### Installation

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/your-github-username/discord-transcription-bot.git
   cd discord-transcription-bot
   ```

2. **Create a Virtual Environment (optional but recommended):**

   ```bash
   python -m venv venv
   # Activate the virtual environment
   # On Windows: venv\Scripts\activate
   # On macOS/Linux: source venv/bin/activate
   ```

3. **Install Dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Environment Variables:**

   Create a `.env` file in the root directory and add your Discord bot token and guild ID:

   ```
   DISCORD_TOKEN=your_discord_bot_token
   GUILD_ID=your_guild_id
   PLAYER_MAP_FILE_PATH=path_to_player_map.yml
   TRANSCRIPTION_LANGUAGE=auto
   ```

### Configuration

- Edit `player_map.yml` to map Discord user IDs to speaker labels for transcription.
- Adjust transcription settings in `src/sinks/whisper_sink.py` if needed.

## Usage

1. **Start the Bot:**

   ```bash
   python main.py
   ```

2. **Bot Commands:**

   - `/connect`: Connect VOLO to your voice channel.
   - `/language`: Sets transcription language (`auto`, `de`, `eng`).
   - `/start_recording`: Starts the transcription in the current voice channel.
   - `/stop_recording`: Stops the transcription.
   - `/disconnect`: Disconnects the bot from the voice channel.

## Contributing

Contributions to this project are welcome. Please ensure to follow the project's coding style and submit pull requests for any new features or bug fixes.

## License

[MIT License](LICENSE)

## Acknowledgments

- This project uses [Whisper](https://github.com/openai/whisper) for audio transcription.
- Thanks to the Discord.py community for their support and resources.

## Session recording flow

Entry points in code:
- `/start_recording` start command is implemented in `main.py` (`start_recording` function).
- `/stop_recording` stop command is implemented in `main.py` (`stop_recording` function).
- The per-chunk transcription callback path is `WhisperSink.insert_voice()` -> `WhisperSink.write_transcription_log()` in `src/sinks/whisper_sink.py`, drained in `VoloBot.get_transcription()`.
