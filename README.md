# MONAH: Multi-Modal Narratives for Humans

## Problem

Analyzing videos in video format (visual + audio + text) needs a lot of human expertise and end-to-end deep learning
methods are less interpretable.

## Solution

Inspired by how the linguistics community analyze conversations using the Jefferson transcription system. MONAH creates
a multi-modal text narrative for dyadic (two-people) video-recorded conversations by weaving _what_ is being said with _
how_ its being said.

# ScreenCast

To add later

# Required Inputs

Two videos, one for each speaker. Works best when the camera is in front of the speaker, instead of from an angle.
Verbatim Transcript from YouTube.

# User Interface

Text menu based for easy configuration.

![alt text](https://lucid.app/publicSegments/view/57060778-69b4-4b96-8a6a-2fa7016d2c23/image.jpeg?raw=true)

# Support modalities in the narratives
![alt text](https://lucid.app/publicSegments/view/eed6165d-fd5d-4af5-a484-56693fe1ca1e/image.jpeg?raw=true)


# Output - MONAH Narrative
To add later


# Dependencies (Technology Stack)
To add as we build this repo up.
## Fine Narratives
Actions
- OpenFace 2.2.0
https://github.com/TadasBaltrusaitis/OpenFace
  

Prosody
- Vokaturi 3.x
https://developers.vokaturi.com/downloads/sdk

## Coarse Narratives
Demographics
- IBM Watson (Deprecated 1 Dec 2021)
https://cloud.ibm.com/docs/personality-insights


Semantics
- Sentiment
- Questions

Mimicry
- Dynamic Time Wrapping




# Contributions
MOANH is meant to be a modular system that allows for additions to be simple. Joshua to add architectural diagram.

## Pipeline (Intermediate Artifacts)
To add later

## Continuous Integration
Joshua to add PyLint Python Style Tests
Joshua to add Compulsory Unit Tests




# Citation
If you find MONAH useful in any of your publications we ask you to cite the following:

Features introduced in Paper 1 are in white, features introduced in paper 2 are in blue.

![alt text](https://lucid.app/publicSegments/view/65ae8f82-2972-4ce7-aedc-54dcf7af47d2/image.jpeg?raw=true)

- Paper 1 (white features) Kim, J. Y., Kim, G. Y., & Yacef, K. (2019). Detecting depression in dyadic conversations with multimodal narratives and visualizations. In Australasian Joint Conference on Artificial Intelligence (pp. 303-314). Springer, Cham.
- Paper 2 (blue features) Kim, J. Y., Yacef, K., Kim, G., Liu, C., Calvo, R., & Taylor, S. (2021, April). MONAH: Multi-Modal Narratives for Humans to analyze conversations. In Proceedings of the 16th Conference of the European Chapter of the Association for Computational Linguistics: Main Volume (pp. 466-479).

