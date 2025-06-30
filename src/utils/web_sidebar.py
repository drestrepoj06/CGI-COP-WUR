import streamlit as st

import streamlit as st
import requests
import base64


def get_encoded_logo(url):
    response = requests.get(url)
    if response.status_code == 200:
        return base64.b64encode(response.content).decode()
    else:
        return None


def add_logo():
    url1 = "https://raw.githubusercontent.com/drestrepoj06/CGI-COP-WUR/main/src/vec/CGI_logo.png"
    url2 = "https://raw.githubusercontent.com/drestrepoj06/CGI-COP-WUR/main/src/vec/WUR_RGB_standard_2021.png"

    link1 = "https://www.cgi.com/nl/nl"
    link2 = "https://www.wur.nl/"

    encoded1 = get_encoded_logo(url1)
    encoded2 = get_encoded_logo(url2)

    if encoded1 and encoded2:
        st.markdown(
            f"""
            <div style="text-align: center; padding: 20px 0;">
                <a href="{link1}" target="_blank">
                    <img src="data:image/png;base64,{encoded1}" style="height: 50px;">
                </a><br><br>
                <a href="{link2}" target="_blank">
                    <img src="data:image/png;base64,{encoded2}" style="height: 80px;">
                </a>
            </div>
            """,
            unsafe_allow_html=True
        )
    else:
        st.error("Failed to load one or both logos.")


def render_sidebar():
    st.title("RCOP Railway Operations Center for Meldkamer Spoor")

    st.markdown("""
    🚨 **Emergency Simulation Dashboard**

    This system allows you to simulate railway incidents and visualize the coordination of emergency response units.

    Use the control panel on the right to simulate or resolve incidents in real time.
    """)

    st.markdown("""
    🔄 Data refreshes automatically with each action.  
    🗺️ Maps are used for real-time geospatial rendering.
    """)

    add_logo()

    display_linkedin_team()

    st.markdown("---")


def display_linkedin_team():
    # 样式定义
    st.markdown("""
        <style>
        .circle-image {
            width: 80px;
            height: 80px;
            border-radius: 50%;
            overflow: hidden;
            box-shadow: 0 0 6px rgba(0, 0, 0, 0.2);
            margin: 0 auto;
        }
        .circle-image img {
            width: 100%;
            height: 100%;
            object-fit: cover;
        }
        .name-link {
            text-decoration: none;
            color: #333;
            font-size: 13px;
            display: block;
            margin-top: 6px;
        }
        .team-container {
            display: flex;
            justify-content: center;
            gap: 30px;
            margin-top: 20px;
            flex-wrap: wrap;
        }
        .member-box {
            text-align: center;
            width: 100px;
        }
        </style>
    """, unsafe_allow_html=True)

    # HTML 内容拼接
    linkedin_profiles = [
        {
            "name": "Máté",
            "url": "https://www.linkedin.com/in/torokmate/",
            "avatar": "https://raw.githubusercontent.com/drestrepoj06/CGI-COP-WUR/main/src/avatars/mate.jpeg"
        },
        {
            "name": "Jhon",
            "url": "https://www.linkedin.com/in/jhonrestrepogeologist/",
            "avatar": "https://raw.githubusercontent.com/drestrepoj06/CGI-COP-WUR/main/src/avatars/jhon.jpeg"
        },
        {
            "name": "Thijs",
            "url": "https://www.linkedin.com/in/thijs-vons-3294612a0/",
            "avatar": "https://raw.githubusercontent.com/drestrepoj06/CGI-COP-WUR/main/src/avatars/thijs.jpeg"
        },
        {
            "name": "Falco",
            "url": "https://www.linkedin.com/in/falco-latour/",
            "avatar": "https://raw.githubusercontent.com/drestrepoj06/CGI-COP-WUR/main/src/avatars/falco.jpeg"
        },
        {
            "name": "Xiaolu",
            "url": "https://www.linkedin.com/in/xlyan1024/",
            "avatar": "https://raw.githubusercontent.com/drestrepoj06/CGI-COP-WUR/main/src/avatars/xiaolu.jpeg"
        },
    ]

    blocks = []
    for p in linkedin_profiles:
        block = f"""
        <div class='member-box'>
            <a href="{p['url']}" target="_blank" title="{p['name']}">
                <div class='circle-image'>
                    <img src="{p['avatar']}"/>
                </div>
            </a>
            <a class='name-link' href="{p['url']}" target="_blank">{p['name']}</a>
        </div>
        """
        blocks.append(block.strip())

    html = "<div class='team-container'>" + "".join(blocks) + "</div>"

    st.markdown("### Meet the Team 👥")
    st.markdown(html, unsafe_allow_html=True)
