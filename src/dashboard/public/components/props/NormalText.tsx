import React from 'react';
import {Text, View} from 'react';

interface NormalTextProps {
    textHeight?: number;
    textWidth?: number;
    textSize?: number;
    text?: string;
    textWeight?: string;
}

const NormalText: React.FC<NormalTextProps> = ({textHeight, textWidth, textSize, text, textWeight}) => {
    const textStyle = {
        height: textHeight,
        width: textWidth,
        fontSize: textSize,
        weight: textWeight
    };

    return (
        <View>
            <Text style={textStyle}>{text}</Text>
        </View>
    );
};

export default NormalText;
