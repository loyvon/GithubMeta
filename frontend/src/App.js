import React, { useState } from 'react';
import axios from 'axios';
import base64 from 'react-native-base64';

function App() {
  const [inputValue, setInputValue] = useState('');
  const [response, setResponse] = useState('');

  const handleInputChange = (e) => {
    setInputValue(e.target.value);
  };

  const handleClick = async () => {
    // Replace with your server URL and endpoint
    const res = await axios.get(`/api/search?question=${inputValue}`);
    setResponse(res.data);
  };

  return (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', height: '100vh' }}>
      <h1>Github Meta</h1>
      <div style={{ display: 'flex', marginBottom: '2vh' }}>
        <input type="text" placeholder="Question..." style={{ padding: '10px', fontSize: '16px', width: '80vw', border: 'none',
      boxShadow: '0px 0.3vh 0.6vh 0px rgba(0,0,0,0.16)' }} onChange={handleInputChange} />
        <button onClick={handleClick} style={{ marginLeft: '1vw', borderRadius: '1.2vh' }}>Get Answer</button>
      </div>
      {response &&
        <div style={{ maxHeight: '80vh', overflow: 'auto', width: '100%', display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
          {response.image && <img src={`data:image/jpeg;base64,${response.image}`} alt="response-image" style={{ maxWidth: '80%' }}/>}
          <p>{response.explanation}</p>
        </div>
      }
    </div>
  );
}

export default App;
