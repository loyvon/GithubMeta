import React, { useState } from 'react';
import axios from 'axios';

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
      <div style={{ display: 'flex', marginBottom: '20px' }}>
        <input type="text" placeholder="Question..." style={{ padding: '10px', fontSize: '16px', width: '1000px', border: 'none',
      boxShadow: '0px 3px 6px 0px rgba(0,0,0,0.16)' }} onChange={handleInputChange} />
        <button onClick={handleClick} style={{ marginLeft: '10px', borderRadius: '12px' }}>Get Answer</button>
      </div>
      {response && <p>{response}</p>}
    </div>
  );
}

export default App;
